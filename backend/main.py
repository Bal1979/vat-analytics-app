#!/usr/bin/env python3
"""
VAT Analytics API
Momsanalyse fra Excel/CSV data — 103 automatiserede tests.
"""

import os
import re
import uuid
import secrets
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from parsers.excel_parser import parse_excel, get_column_mapping_preview
from analytics.engine import run_analytics

app = FastAPI(
    title="VAT Analytics API",
    description="Momsanalyse fra Excel/CSV data — 103 automatiserede tests baseret på Skattestyrelsens kontrolmetoder",
    version="0.1.0",
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

MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "50")) * 1024 * 1024


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

_cors_origins = os.environ.get(
    "CORS_ORIGINS", "https://vat.balai.dk,http://localhost:3000"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Serve static files
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".tsv"}


def _save_upload(file: UploadFile) -> str:
    """Gem uploadet fil og returnér filsti."""
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

    # Stream file in chunks with size validation
    total_size = 0
    chunk_size = 1024 * 1024  # 1 MB chunks
    with open(file_path, "wb") as f:
        while True:
            chunk = file.file.read(chunk_size)
            if not chunk:
                break
            total_size += len(chunk)
            if total_size > MAX_UPLOAD_BYTES:
                f.close()
                os.remove(file_path)
                raise HTTPException(
                    413,
                    f"Filen er for stor. Maksimal filstørrelse er {MAX_UPLOAD_BYTES // (1024 * 1024)} MB.",
                )
            f.write(chunk)

    if total_size == 0:
        os.remove(file_path)
        raise HTTPException(400, "Filen er tom. Upload venligst en fil med indhold.")

    return file_path


def _cleanup(file_path: str):
    """Slet uploadet fil."""
    if file_path and os.path.exists(file_path):
        os.remove(file_path)


@app.get("/health")
def health():
    return {"status": "ok", "service": "VAT Analytics", "version": "0.1.0"}


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
    file_path = _save_upload(file)
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
    Returnerer scores, findings og drill-down data.
    """
    file_path = _save_upload(file)
    try:
        # Parse Excel/CSV til standardformat
        parsed_data = parse_excel(file_path)

        if parsed_data.get("parse_info", {}).get("error"):
            raise HTTPException(400, parsed_data["parse_info"]["error"])

        # Kør analytics engine (samme som SAF-T Analytics)
        results = run_analytics(parsed_data)

        return JSONResponse({
            "filename": file.filename,
            "parse_info": parsed_data["parse_info"],
            "header": parsed_data["header"],
            "analytics": results,
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Fejl ved analyse: {str(e)}")
    finally:
        _cleanup(file_path)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5003))
    uvicorn.run(app, host="0.0.0.0", port=port)
