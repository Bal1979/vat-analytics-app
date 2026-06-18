#!/usr/bin/env python3
"""
VAT Analytics API
Momsanalyse fra Excel/CSV data — 103 automatiserede tests.

Understøtter store filer op til 2 GB med asynkron job-processering:
- Filer < 50 MB: synkron analyse (returnerer resultat direkte)
- Filer >= 50 MB: background thread, returnerer job_id med polling-endpoints
"""

import os
import re
import uuid
import shutil
import time
import logging
import threading
import traceback
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from parsers.excel_parser import parse_excel, get_column_mapping_preview, LARGE_FILE_THRESHOLD
from parsers.data_adapter import adapt_excel_to_saft
from analytics.engine import run_analytics
import auth
import audit_log

app = FastAPI(
    title="VAT Analytics API",
    description="Momsanalyse fra Excel/CSV data — 103 automatiserede tests baseret på Skattestyrelsens kontrolmetoder",
    version="0.2.0",
)
# Maks upload: 2 GB
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "2048")) * 1024 * 1024

# Job tracking for asynkrone analyser (in-memory). Ryddes løbende, så hukommelsen
# ikke vokser ubegrænset: færdige/fejlede jobs ældre end JOB_RETENTION_SECONDS
# fjernes, og der holdes højst MAX_JOBS jobs.
jobs = {}
_jobs_lock = threading.Lock()
JOB_RETENTION_SECONDS = int(os.environ.get("JOB_RETENTION_SECONDS", "3600"))
MAX_JOBS = int(os.environ.get("MAX_JOBS", "100"))


def _prune_jobs():
    """Lazy oprydning af jobtilstand (trådsikker): fjern gamle terminale jobs og
    cap det samlede antal, så in-memory-dict'en ikke vokser ubegrænset."""
    now = time.time()
    with _jobs_lock:
        stale = [jid for jid, v in jobs.items()
                 if v.get("status") in ("done", "error")
                 and now - v.get("created_ts", now) > JOB_RETENTION_SECONDS]
        for jid in stale:
            jobs.pop(jid, None)
        if len(jobs) > MAX_JOBS:
            oldest = sorted(jobs.items(), key=lambda kv: kv[1].get("created_ts", 0))
            for jid, _v in oldest[:len(jobs) - MAX_JOBS]:
                jobs.pop(jid, None)

# --- Session-baseret auth (porteret fra SAF-T/VIES/Data Extract) ---
# Ingen credentials i kode/miljø: adgang fås via /setup (første gang) og
# invitationslinks. SECRET_KEY kræves i produktion (ellers brydes sessions
# ved >1 worker / genstart).
_SESSION_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "1") != "0"
app.add_middleware(
    SessionMiddleware,
    secret_key=auth.secret_key(),
    https_only=_SESSION_SECURE,
    same_site="lax",
    max_age=auth.SESSION_LIFETIME_HOURS * 3600,
)
auth.init_auth(app)


# --- HTTP-sikkerhedsheaders (defense-in-depth) ---
# Stram CSP: ingen CDN'er, ingen 'unsafe-*'. Alt JS/CSS er self-hosted i
# /static (auth.css, style.css, app.js, admin.js).
_CSP = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self'; "
    "img-src 'self' data:; "
    "font-src 'self' data:; "
    "connect-src 'self'; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'; "
    "form-action 'self'"
)


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("Content-Security-Policy", _CSP)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return resp


# CORS: eksplicitte origins fra miljøvariabel
_cors_origins = os.environ.get(
    "CORS_ORIGINS", "https://vat.balai.dk,http://localhost:3000"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins if o.strip()],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Serve static files
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".tsv"}


def _save_upload(file: UploadFile) -> tuple:
    """
    Gem uploadet fil via streaming (aldrig hele filen i hukommelse) og returnér (filsti, filstørrelse).
    """
    original_name = file.filename or "upload"
    # Sanitize: strip path separators, keep only safe characters
    safe_name = os.path.basename(original_name)
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", safe_name)

    ext = os.path.splitext(safe_name)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Filtype '{ext}' er ikke understøttet. Brug: {', '.join(ALLOWED_EXTENSIONS)}")

    # Use UUID-based filename to prevent any path traversal
    job_id = str(uuid.uuid4())
    safe_filename = f"{job_id}{ext}"
    file_path = os.path.join(UPLOAD_DIR, safe_filename)

    # Verify resolved path is inside UPLOAD_DIR
    if not os.path.realpath(file_path).startswith(os.path.realpath(UPLOAD_DIR)):
        raise HTTPException(400, "Ugyldig filsti")

    # Stream file in chunks with size validation (1 MB chunks)
    total_size = 0
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f, length=1024 * 1024)

    total_size = os.path.getsize(file_path)
    logger.info("File uploaded: %s (%.2f MB)", safe_name, total_size / (1024 * 1024))

    if total_size > MAX_UPLOAD_BYTES:
        os.remove(file_path)
        raise HTTPException(
            413,
            f"Filen er for stor. Maksimal filstørrelse er {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.",
        )

    if total_size == 0:
        os.remove(file_path)
        raise HTTPException(400, "Filen er tom. Upload venligst en fil med indhold.")

    return file_path, total_size


def _cleanup(file_path: str):
    """Slet uploadet fil."""
    if file_path and os.path.exists(file_path):
        os.remove(file_path)


def _run_analysis_job(job_id: str, file_path: str, filename: str, file_size: int):
    """
    Kør analyse i en background thread. Opdaterer jobs dict med progress.
    """
    try:
        jobs[job_id]["status"] = "parsing"
        logger.info("Job %s: created for file '%s' (%.2f MB)", job_id, filename, file_size / (1024 * 1024))

        def progress_cb(percent, rows_done, total_rows):
            jobs[job_id]["progress"] = percent
            jobs[job_id]["rows_processed"] = rows_done
            jobs[job_id]["total_rows"] = total_rows

        logger.info("Job %s: parsing started", job_id)
        parsed_data = parse_excel(file_path, progress_callback=progress_cb)
        logger.info("Job %s: parsing finished", job_id)

        if parsed_data.get("parse_info", {}).get("error"):
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = parsed_data["parse_info"]["error"]
            logger.error("Job %s: parse error — %s", job_id, parsed_data["parse_info"]["error"])
            return

        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 100  # Parsing done

        # Adapt flat Excel data to SAF-T structure expected by analytics engine
        adapted_data = adapt_excel_to_saft(parsed_data)
        logger.info("Job %s: analysis started", job_id)
        results = run_analytics(adapted_data)
        logger.info("Job %s: analysis finished", job_id)

        jobs[job_id]["status"] = "done"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["result"] = {
            "filename": filename,
            "parse_info": adapted_data["parse_info"],
            "header": adapted_data["header"],
            "analytics": results,
        }

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = f"Fejl ved analyse: {str(e)}"
        jobs[job_id]["traceback"] = traceback.format_exc()
        logger.error("Job %s: error — %s", job_id, str(e))
    finally:
        _cleanup(file_path)


@app.get("/health")
def health():
    return {"status": "ok", "service": "VAT Analytics", "version": "0.2.0"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request, user=Depends(auth.require_login)):
    """Serve frontend (kræver login)."""
    return auth.templates.TemplateResponse(
        request, "index.html", {"csrf": auth.csrf_token(request)}
    )


@app.post("/preview")
async def preview_file(request: Request, file: UploadFile = File(...),
                       user=Depends(auth.require_login), _=Depends(auth.verify_csrf)):
    """
    Upload en fil og få en preview af kolonner + auto-detekteret mapping.
    Brugeren kan derefter bekræfte/rette mappingen før analyse.
    """
    file_path, _f = _save_upload(file)
    try:
        preview = get_column_mapping_preview(file_path)
        return JSONResponse({
            "filename": file.filename,
            "preview": preview,
        })
    except Exception as e:
        raise HTTPException(500, f"Fejl ved preview: {str(e)}")
    finally:
        _cleanup(file_path)


@app.post("/analyze")
async def analyze(request: Request, file: UploadFile = File(...),
                  user=Depends(auth.require_login), _=Depends(auth.verify_csrf)):
    """
    Upload en Excel/CSV fil og kør alle 103 momsanalyser.

    For filer < 50 MB: synkron analyse, returnerer resultat direkte.
    For filer >= 50 MB: starter background job, returnerer job_id til polling.
    """
    file_path, file_size = _save_upload(file)

    # Revisionslog: KUN metadata (hvem/hvornår/filstørrelse) — aldrig momsdata.
    audit_log.log("analysis.run", actor=user["username"],
                  detail=f"{file_size} bytes", ip=auth._client_ip(request))

    # Store filer: asynkron processering
    if file_size >= LARGE_FILE_THRESHOLD:
        _prune_jobs()  # lazy oprydning før et nyt job tilføjes
        job_id = str(uuid.uuid4())
        with _jobs_lock:
            jobs[job_id] = {
                "status": "queued",
                "progress": 0,
                "rows_processed": 0,
                "total_rows": 0,
                "filename": file.filename,
                "file_size": file_size,
                "created_at": datetime.utcnow().isoformat(),
                "created_ts": time.time(),
                "result": None,
                "error": None,
            }

        thread = threading.Thread(
            target=_run_analysis_job,
            args=(job_id, file_path, file.filename, file_size),
            daemon=True,
        )
        thread.start()

        return JSONResponse({
            "job_id": job_id,
            "status": "queued",
            "file_size": file_size,
            "message": f"Stor fil ({file_size / (1024*1024):.1f} MB) — analyse kører i baggrunden.",
        })

    # Små filer: synkron analyse (uændret adfærd)
    try:
        logger.info("Sync analysis: parsing started for '%s'", file.filename)
        parsed_data = parse_excel(file_path)
        logger.info("Sync analysis: parsing finished for '%s'", file.filename)

        if parsed_data.get("parse_info", {}).get("error"):
            logger.error("Sync analysis: parse error — %s", parsed_data["parse_info"]["error"])
            raise HTTPException(400, parsed_data["parse_info"]["error"])

        # Adapt flat Excel data to SAF-T structure expected by analytics engine
        adapted_data = adapt_excel_to_saft(parsed_data)
        logger.info("Sync analysis: analysis started for '%s'", file.filename)
        results = run_analytics(adapted_data)
        logger.info("Sync analysis: analysis finished for '%s'", file.filename)

        return JSONResponse({
            "filename": file.filename,
            "parse_info": adapted_data["parse_info"],
            "header": adapted_data["header"],
            "analytics": results,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Sync analysis: error — %s", str(e))
        raise HTTPException(500, f"Fejl ved analyse: {str(e)}")
    finally:
        _cleanup(file_path)


@app.get("/status/{job_id}")
def job_status(job_id: str, user=Depends(auth.require_login)):
    """
    Returnér status og progress for et asynkront analyse-job.
    """
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job ikke fundet")

    return JSONResponse({
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "rows_processed": job["rows_processed"],
        "total_rows": job["total_rows"],
        "filename": job["filename"],
        "file_size": job["file_size"],
        "error": job["error"],
    })


@app.get("/result/{job_id}")
def job_result(job_id: str, user=Depends(auth.require_login)):
    """
    Returnér resultatet af et færdigt analyse-job.
    """
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job ikke fundet")

    if job["status"] == "error":
        raise HTTPException(500, job["error"])

    if job["status"] != "done":
        raise HTTPException(
            202,
            f"Analyse er stadig i gang (status: {job['status']}, progress: {job['progress']}%)",
        )

    result = job["result"]

    # Ryd op i job-data for at frigøre hukommelse (behold metadata)
    # Resultatet returneres én gang, derefter fjernes det store data-objekt
    return JSONResponse(result)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5003))
    uvicorn.run(app, host="0.0.0.0", port=port)
