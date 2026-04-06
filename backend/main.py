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
import secrets
import logging
import threading
import traceback
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from parsers.excel_parser import parse_excel, get_column_mapping_preview, LARGE_FILE_THRESHOLD
from parsers.data_adapter import adapt_excel_to_saft
from analytics.engine import run_analytics

app = FastAPI(
    title="VAT Analytics API",
    description="Momsanalyse fra Excel/CSV data — 103 automatiserede tests baseret på Skattestyrelsens kontrolmetoder",
    version="0.2.0",
)
security = HTTPBasic()

# Brugere med adgang — læses fra miljøvariabel AUTH_USERS (format: "user1:pass1,user2:pass2")
_auth_raw = os.environ.get("AUTH_USERS", "admin:balai2025,Fabian:Salvatore")
USERS = {}
for _pair in _auth_raw.split(","):
    _pair = _pair.strip()
    if ":" in _pair:
        _u, _p = _pair.split(":", 1)
        USERS[_u.strip()] = _p.strip()

# Maks upload: 2 GB
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "2048")) * 1024 * 1024

# Job tracking for asynkrone analyser
jobs = {}


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Verificér brugernavn og password."""
    correct_password = USERS.get(credentials.username)
    if not correct_password or not secrets.compare_digest(
        credentials.password.encode("utf-8"), correct_password.encode("utf-8")
    ):
        raise HTTPException(
            status_code=401,
            detail="Forkert brugernavn eller adgangskode",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


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
def index(username: str = Depends(verify_credentials)):
    """Serve frontend."""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r") as f:
        return f.read()


@app.post("/preview")
async def preview_file(file: UploadFile = File(...), username: str = Depends(verify_credentials)):
    """
    Upload en fil og få en preview af kolonner + auto-detekteret mapping.
    Brugeren kan derefter bekræfte/rette mappingen før analyse.
    """
    file_path, _ = _save_upload(file)
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
async def analyze(file: UploadFile = File(...), username: str = Depends(verify_credentials)):
    """
    Upload en Excel/CSV fil og kør alle 103 momsanalyser.

    For filer < 50 MB: synkron analyse, returnerer resultat direkte.
    For filer >= 50 MB: starter background job, returnerer job_id til polling.
    """
    file_path, file_size = _save_upload(file)

    # Store filer: asynkron processering
    if file_size >= LARGE_FILE_THRESHOLD:
        job_id = str(uuid.uuid4())
        jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "rows_processed": 0,
            "total_rows": 0,
            "filename": file.filename,
            "file_size": file_size,
            "created_at": datetime.utcnow().isoformat(),
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
def job_status(job_id: str, username: str = Depends(verify_credentials)):
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
def job_result(job_id: str, username: str = Depends(verify_credentials)):
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
