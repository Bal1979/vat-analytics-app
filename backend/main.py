#!/usr/bin/env python3
"""
VAT Analytics API
Momsanalyse fra Excel/CSV data — 103 automatiserede tests.
"""

import os
import uuid
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from parsers.excel_parser import parse_excel, get_column_mapping_preview
from analytics.engine import run_analytics

app = FastAPI(
    title="VAT Analytics API",
    description="Momsanalyse fra Excel/CSV data — 103 automatiserede tests baseret på Skattestyrelsens kontrolmetoder",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".tsv"}


def _save_upload(file: UploadFile) -> str:
    """Gem uploadet fil og returnér filsti."""
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Filtype '{ext}' er ikke understøttet. Brug: {', '.join(ALLOWED_EXTENSIONS)}")

    job_id = str(uuid.uuid4())[:8]
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    return file_path


def _cleanup(file_path: str):
    """Slet uploadet fil."""
    if file_path and os.path.exists(file_path):
        os.remove(file_path)


@app.get("/health")
def health():
    return {"status": "ok", "service": "VAT Analytics", "version": "0.1.0"}


@app.post("/preview")
async def preview_file(file: UploadFile = File(...)):
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
async def analyze(file: UploadFile = File(...)):
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
