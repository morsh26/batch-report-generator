#!/usr/bin/env python3
"""
Server for Financial Reports Service.

Async API that accepts PDF URLs, processes in background, and stores results in Supabase.

Flow:
1. Edge function POSTs request with report_id + PDF URLs
2. Server immediately returns "Processing started"
3. Background task downloads PDFs, generates report
4. When done: uploads to Supabase bucket + updates status to COMPLETED

Usage:
    uvicorn server:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, HttpUrl
from typing import Optional
import logging
import httpx
import time
import os

import tempfile
from pathlib import Path

# Import all core functionality
from core import (
    SECTIONS,
    BOARD_REPORT_SECTIONS,
    SECTION_DISPLAY_NAMES,
    MODEL_NAME,
    GOOGLE_API_KEY,
    API_DELAY,
    validate_config,
    HEBREW_MONTHS,
    configure_gemini,
    upload_pdf_to_gemini,
    generate_section_with_fallback,
    is_heavy_report,
    map_report_structure,
    create_report_slices,
    assemble_report,
    html_to_pdf,
    extract_holding_chart_page,
    create_holding_chart_html,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =============================================================================
# SUPABASE CONFIGURATION
# =============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # Service key for server-side ops
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "reports")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "reports")

# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="Financial Reports Service",
    description="Async API for generating financial reports from PDF URLs",
    version="2.0.0"
)

# Global model instance
_model = None

# HTTP client timeout (seconds)
DOWNLOAD_TIMEOUT = 120


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    model: str
    supabase_configured: bool


class GenerateReportRequest(BaseModel):
    """Request body for report generation."""
    report_id: str
    company_name: str
    annual_report_url: HttpUrl
    quarterly_report_url: Optional[HttpUrl] = None


class GenerateReportResponse(BaseModel):
    """Immediate response when processing starts."""
    status: str
    report_id: str
    message: str


# =============================================================================
# SUPABASE HELPERS
# =============================================================================

async def update_report_status(report_id: str, status: str, failure_reason: Optional[str] = None):
    """Update report status in Supabase table."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("Supabase not configured, skipping status update")
        return

    try:
        async with httpx.AsyncClient() as client:
            data = {"status": status}
            if failure_reason:
                data["failure_reason"] = failure_reason

            response = await client.patch(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?id=eq.{report_id}",
                json=data,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal"
                }
            )
            response.raise_for_status()
            logger.info(f"Updated report {report_id} status to: {status}")

    except Exception as e:
        logger.error(f"Failed to update report status: {e}")


async def upload_to_supabase(report_id: str, filename: str, content: bytes, content_type: str):
    """Upload file to Supabase storage bucket."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("Supabase not configured, skipping upload")
        return None

    try:
        file_path = f"{report_id}/{filename}"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{file_path}",
                content=content,
                headers={
                    "apikey": SUPABASE_SERVICE_KEY,
                    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    "Content-Type": content_type,
                    "x-upsert": "true"  # Overwrite if exists
                }
            )
            response.raise_for_status()

            # Return public URL
            public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{file_path}"
            logger.info(f"Uploaded {filename} to Supabase: {public_url}")
            return public_url

    except Exception as e:
        logger.error(f"Failed to upload to Supabase: {e}")
        return None


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def download_pdf(url: str) -> bytes:
    """Download a PDF from a URL."""
    logger.info(f"Downloading PDF from: {url}")

    try:
        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
            response = await client.get(str(url))
            response.raise_for_status()
            logger.info(f"Downloaded {len(response.content):,} bytes")
            return response.content

    except httpx.TimeoutException:
        raise Exception(f"Timeout downloading PDF from {url}")
    except httpx.HTTPStatusError as e:
        raise Exception(f"Failed to download PDF: HTTP {e.response.status_code}")
    except Exception as e:
        raise Exception(f"Failed to download PDF: {str(e)}")


def get_filename_from_url(url: str) -> str:
    """Extract filename from URL."""
    return url.split('/')[-1].split('?')[0] or "report.pdf"


# =============================================================================
# BACKGROUND PROCESSING
# =============================================================================

async def process_report_async(request: GenerateReportRequest):
    """
    Background task to process report and upload to Supabase.
    """
    report_id = request.report_id
    company_name = request.company_name

    logger.info(f"=== Background processing started for report {report_id} ===")

    try:
        # Update status to processing
        await update_report_status(report_id, "processing")

        # Step 1: Download PDFs
        logger.info("Step 1: Downloading PDFs...")
        annual_bytes = await download_pdf(str(request.annual_report_url))
        annual_filename = get_filename_from_url(str(request.annual_report_url))

        quarterly_bytes = None
        if request.quarterly_report_url:
            quarterly_bytes = await download_pdf(str(request.quarterly_report_url))

        # Step 2: Check if heavy report
        logger.info("Step 2: Checking report size...")
        is_heavy, total_pages = is_heavy_report(annual_bytes)

        board_uri = None
        financial_uri = None

        # Step 3: Process based on report size
        if is_heavy:
            logger.warning(f"⚠️  HEAVY REPORT ({total_pages} pages)")
            logger.info("Step 3: Mapping structure and creating slices...")

            structure_map = map_report_structure(annual_bytes, _model, annual_filename)
            slices = create_report_slices(annual_bytes, structure_map)

            board_slice_bytes = slices.get('board_slice')
            financial_slice_bytes = slices.get('financial_slice')

            if board_slice_bytes:
                board_uri = upload_pdf_to_gemini(board_slice_bytes, f"board_{annual_filename}")
            if financial_slice_bytes:
                financial_uri = upload_pdf_to_gemini(financial_slice_bytes, f"financial_{annual_filename}")

            if not board_uri and not financial_uri:
                logger.warning("Slicing failed, falling back to standard processing")
                is_heavy = False

        if not is_heavy:
            logger.info(f"Step 3: Standard report ({total_pages} pages), uploading...")
            primary_uri = upload_pdf_to_gemini(annual_bytes, annual_filename)
            if not primary_uri:
                raise Exception("Failed to upload PDF to Gemini")
            board_uri = primary_uri
            financial_uri = primary_uri

        # Upload quarterly if provided
        quarterly_uri = None
        if quarterly_bytes:
            quarterly_uri = upload_pdf_to_gemini(quarterly_bytes, "quarterly_report.pdf")

        # Step 4: Extract holding chart
        logger.info("Step 4: Extracting holding chart...")
        holding_chart_html = create_holding_chart_html(None, company_name)

        if GOOGLE_API_KEY:
            with tempfile.TemporaryDirectory() as temp_dir:
                holding_chart_path = extract_holding_chart_page(
                    pdf_bytes=annual_bytes,
                    output_dir=Path(temp_dir),
                    google_api_key=GOOGLE_API_KEY,
                    company_name=company_name
                )
                holding_chart_html = create_holding_chart_html(holding_chart_path, company_name)

        # Step 5: Generate all sections
        logger.info("Step 5: Generating report sections...")
        html_sections = []
        failed_sections = []

        for section_id in SECTIONS:
            logger.info(f"  Generating: {SECTION_DISPLAY_NAMES.get(section_id, section_id)}")

            if is_heavy:
                if section_id in BOARD_REPORT_SECTIONS:
                    section_uri = board_uri or financial_uri
                else:
                    section_uri = financial_uri or board_uri
            else:
                section_uri = board_uri

            section_html = generate_section_with_fallback(
                section_id=section_id,
                primary_uri=section_uri,
                secondary_uri=quarterly_uri,
                fallback_uri=section_uri,
                company_name=company_name
            )

            html_sections.append(section_html)

            if section_id == 'company_profile':
                html_sections.append(holding_chart_html)

            if 'class="error"' in section_html:
                failed_sections.append(section_id)

            time.sleep(API_DELAY)

        # Step 6: Assemble final report
        logger.info("Step 6: Assembling final report...")
        final_html = assemble_report(company_name, html_sections)

        # Step 7: Convert to PDF
        logger.info("Step 7: Converting to PDF...")
        pdf_bytes = html_to_pdf(final_html)

        # Step 8: Upload to Supabase
        logger.info("Step 8: Uploading to Supabase...")

        # Upload HTML as reports/{report_id}/report.html
        html_url = await upload_to_supabase(
            report_id,
            "report.html",
            final_html.encode('utf-8'),
            "text/html; charset=utf-8"
        )

        # Upload PDF as reports/{report_id}/report.pdf
        pdf_url = None
        if pdf_bytes:
            pdf_url = await upload_to_supabase(
                report_id,
                "report.pdf",
                pdf_bytes,
                "application/pdf"
            )

        # Step 9: Update status to completed
        await update_report_status(report_id, "completed")

        logger.info(f"=== Report {report_id} completed successfully ===")
        if failed_sections:
            logger.warning(f"Failed sections: {failed_sections}")

    except Exception as e:
        logger.error(f"Report {report_id} failed: {e}")
        await update_report_status(report_id, "failed", failure_reason=str(e))


# =============================================================================
# STARTUP
# =============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    global _model

    # Validate core config
    is_valid, errors = validate_config()
    if not is_valid:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        raise RuntimeError("Invalid configuration")

    # Check Supabase config
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("⚠️  Supabase not configured - results won't be uploaded")
    else:
        logger.info(f"Supabase configured: {SUPABASE_URL}")

    try:
        _model = configure_gemini()
        logger.info("Server started successfully")
    except Exception as e:
        logger.error(f"Failed to configure Gemini: {e}")
        raise


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy" if _model else "unhealthy",
        model=MODEL_NAME,
        supabase_configured=bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)
    )


@app.get("/api/v1/sections")
async def list_sections():
    """List available report sections."""
    return {
        "sections": [
            {"id": section_id, "name": SECTION_DISPLAY_NAMES.get(section_id, section_id)}
            for section_id in SECTIONS
        ]
    }


@app.post("/api/v1/reports/generate", response_model=GenerateReportResponse)
async def generate_report(
    request: GenerateReportRequest,
    background_tasks: BackgroundTasks
):
    """
    Start report generation (async).

    Immediately returns "Processing started" and processes in background.
    When complete, uploads HTML + PDF to Supabase and updates status to COMPLETED.

    Request body:
    {
        "report_id": "uuid-123-456",
        "company_name": "מליסרון",
        "annual_report_url": "https://storage.example.com/annual.pdf",
        "quarterly_report_url": "https://storage.example.com/quarterly.pdf"
    }
    """
    if _model is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    logger.info(f"Received request for report_id: {request.report_id}, company: {request.company_name}")

    # Add to background tasks
    background_tasks.add_task(process_report_async, request)

    # Return immediately
    return GenerateReportResponse(
        status="processing",
        report_id=request.report_id,
        message="Processing started"
    )


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "Financial Reports Service",
        "version": "2.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
        "usage": {
            "endpoint": "POST /api/v1/reports/generate",
            "body": {
                "report_id": "uuid-from-supabase",
                "company_name": "שם החברה",
                "annual_report_url": "https://example.com/annual.pdf",
                "quarterly_report_url": "https://example.com/quarterly.pdf (optional)"
            },
            "response": {
                "status": "processing",
                "report_id": "uuid-from-supabase",
                "message": "Processing started"
            },
            "when_done": "Updates Supabase table status=COMPLETED + uploads to bucket"
        }
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
