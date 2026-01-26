#!/usr/bin/env python3
"""
Server for Financial Reports Service.

This is THE CLOUD BOSS - a FastAPI server that exposes the core functionality
as REST API endpoints. This is a placeholder showing how to import from core.

Future endpoints could include:
- POST /api/v1/reports/generate - Generate a report from uploaded PDFs
- POST /api/v1/reports/section - Generate a single section
- GET /api/v1/health - Health check

Usage:
    uvicorn server:app --reload --port 8000
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
import logging

# Import all core functionality (stateless, no file I/O)
from core import (
    # Config
    SECTIONS,
    SECTION_DISPLAY_NAMES,
    MODEL_NAME,
    validate_config,
    # AI Engine
    configure_gemini,
    upload_pdf_to_gemini,
    generate_section_with_fallback,
    # PDF Processor
    is_heavy_report,
    map_report_structure,
    create_report_slices,
    # Report Builder
    assemble_report,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Financial Reports Service",
    description="API for generating financial reports from PDF documents",
    version="1.0.0"
)

# Global model instance (initialized on startup)
_model = None


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class HealthResponse(BaseModel):
    status: str
    model: str
    sections_available: list[str]


class GenerateReportRequest(BaseModel):
    company_name: str


class GenerateReportResponse(BaseModel):
    success: bool
    html: Optional[str] = None
    failed_sections: list[str] = []
    error: Optional[str] = None


# =============================================================================
# STARTUP / SHUTDOWN EVENTS
# =============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize Gemini on startup."""
    global _model

    is_valid, errors = validate_config()
    if not is_valid:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        raise RuntimeError("Invalid configuration")

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
        sections_available=SECTIONS
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
    company_name: str,
    annual_pdf: UploadFile = File(...),
    quarterly_pdf: Optional[UploadFile] = File(None)
):
    """
    Generate a complete financial report from uploaded PDF files.

    This endpoint demonstrates how to use the core modules in a web context:
    1. Accept PDF uploads (bytes)
    2. Call stateless core functions
    3. Return generated HTML

    Args:
        company_name: Name of the company
        annual_pdf: Annual report PDF file
        quarterly_pdf: Optional quarterly report PDF file

    Returns:
        GenerateReportResponse with HTML content or error
    """
    if _model is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Read uploaded files as bytes
        annual_bytes = await annual_pdf.read()

        quarterly_bytes = None
        if quarterly_pdf:
            quarterly_bytes = await quarterly_pdf.read()

        # Check if heavy report
        is_heavy, total_pages = is_heavy_report(annual_bytes)

        # Process based on report size
        if is_heavy:
            # Heavy report: map structure and create slices
            structure_map = map_report_structure(annual_bytes, _model, annual_pdf.filename)
            slices = create_report_slices(annual_bytes, structure_map)

            board_slice_bytes = slices.get('board_slice')
            financial_slice_bytes = slices.get('financial_slice')

            # Upload slices
            board_uri = None
            financial_uri = None

            if board_slice_bytes:
                board_uri = upload_pdf_to_gemini(board_slice_bytes, f"board_{annual_pdf.filename}")
            if financial_slice_bytes:
                financial_uri = upload_pdf_to_gemini(financial_slice_bytes, f"financial_{annual_pdf.filename}")

            primary_uri = board_uri or financial_uri
            if not primary_uri:
                return GenerateReportResponse(
                    success=False,
                    error="Failed to upload PDF slices"
                )
        else:
            # Standard report: upload full file
            primary_uri = upload_pdf_to_gemini(annual_bytes, annual_pdf.filename)
            if not primary_uri:
                return GenerateReportResponse(
                    success=False,
                    error="Failed to upload PDF"
                )
            board_uri = primary_uri
            financial_uri = primary_uri

        # Upload quarterly if provided
        quarterly_uri = None
        if quarterly_bytes:
            quarterly_uri = upload_pdf_to_gemini(quarterly_bytes, quarterly_pdf.filename)

        # Generate all sections
        html_sections = []
        failed_sections = []

        for section_id in SECTIONS:
            # Select appropriate URI for section
            if is_heavy:
                from core import BOARD_REPORT_SECTIONS
                if section_id in BOARD_REPORT_SECTIONS:
                    section_uri = board_uri or financial_uri
                else:
                    section_uri = financial_uri or board_uri
            else:
                section_uri = primary_uri

            section_html = generate_section_with_fallback(
                section_id=section_id,
                primary_uri=section_uri,
                secondary_uri=quarterly_uri,
                fallback_uri=section_uri,
                company_name=company_name
            )

            html_sections.append(section_html)

            if 'class="error"' in section_html:
                failed_sections.append(section_id)

        # Assemble final report
        final_html = assemble_report(company_name, html_sections)

        return GenerateReportResponse(
            success=len(failed_sections) == 0,
            html=final_html,
            failed_sections=failed_sections
        )

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return GenerateReportResponse(
            success=False,
            error=str(e)
        )


@app.get("/")
async def root():
    """Root endpoint with basic info."""
    return {
        "service": "Financial Reports Service",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health"
    }


# =============================================================================
# MAIN (for direct execution)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
