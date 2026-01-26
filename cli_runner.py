#!/usr/bin/env python3
"""
CLI Runner for Financial Reports Service.

This is THE LOCAL BOSS - handles all file system operations:
- Scanning directories for PDFs
- Reading files from disk
- Calling Core modules for processing
- Saving HTML reports to disk

Usage:
    python cli_runner.py [company_filter]

Examples:
    python cli_runner.py                    # Process all companies
    python cli_runner.py "Company Name"     # Process only matching companies
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Optional

# Import all core functionality
from core import (
    # Config
    SECTIONS,
    BOARD_REPORT_SECTIONS,
    API_DELAY,
    DEFAULT_FINANCIAL_REPORTS_DIR,
    DEFAULT_OUTPUT_DIR,
    MODEL_NAME,
    HEAVY_REPORT_THRESHOLD,
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
    # PDF Converter
    html_to_pdf,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# FILE SYSTEM OPERATIONS (CLI-SPECIFIC)
# =============================================================================

def find_pdf_files(company_dir: Path) -> tuple[Optional[Path], Optional[Path]]:
    """
    Find Annual and Quarterly PDF files in a company directory.

    Args:
        company_dir: Path to the company directory

    Returns:
        Tuple of (annual_pdf_path, quarterly_pdf_path)
    """
    annual_pdf = None
    quarterly_pdf = None

    for file in company_dir.glob("*.pdf"):
        filename = file.name.lower()
        if "annual" in filename or "שנתי" in filename:
            annual_pdf = file
        elif any(q in filename for q in ["quarter", "רבעוני", "q1", "q2", "q3", "q4"]):
            quarterly_pdf = file

    # Fallback: use first PDF as annual, second as quarterly
    if annual_pdf is None:
        pdfs = sorted(company_dir.glob("*.pdf"))
        if pdfs:
            annual_pdf = pdfs[0]
            if len(pdfs) > 1:
                quarterly_pdf = pdfs[1]

    return annual_pdf, quarterly_pdf


def read_pdf_bytes(pdf_path: Path) -> Optional[bytes]:
    """
    Read a PDF file and return its contents as bytes.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        PDF content as bytes, or None if error
    """
    try:
        with open(pdf_path, 'rb') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading PDF {pdf_path.name}: {e}")
        return None


def save_html_report(html_content: str, output_path: Path) -> bool:
    """
    Save HTML report to disk.

    Args:
        html_content: The HTML string to save
        output_path: Path where to save the file

    Returns:
        True if successful, False otherwise
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return True
    except Exception as e:
        logger.error(f"Error saving report to {output_path}: {e}")
        return False


def save_pdf_report(pdf_bytes: bytes, output_path: Path) -> bool:
    """
    Save PDF report to disk.

    Args:
        pdf_bytes: The PDF content as bytes
        output_path: Path where to save the file

    Returns:
        True if successful, False otherwise
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(pdf_bytes)
        return True
    except Exception as e:
        logger.error(f"Error saving PDF to {output_path}: {e}")
        return False


# =============================================================================
# COMPANY PROCESSING (ORCHESTRATION)
# =============================================================================

def process_company(company_dir: Path, model) -> tuple[bool, list[str]]:
    """
    Process a single company using Smart Threshold & Dynamic Mapping Strategy.

    This function orchestrates the entire workflow:
    1. Find PDF files in the company directory
    2. Check if heavy report (>300 pages)
    3. If heavy: run AI structure mapping and create slices
    4. Upload PDFs to Gemini
    5. Generate all 8 sections
    6. Assemble and save HTML report

    Args:
        company_dir: Path to the company directory
        model: Configured Gemini model instance

    Returns:
        Tuple of (success, list_of_failed_sections)
    """
    company_name = company_dir.name
    failed_sections = []
    temp_slice_bytes = {}  # Store slice bytes for cleanup tracking

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing company: {company_name}")
    logger.info(f"{'='*60}")

    try:
        # Step 1: Find PDF files
        annual_pdf, quarterly_pdf = find_pdf_files(company_dir)

        if annual_pdf is None:
            logger.error(f"No PDF files found for {company_name}")
            return False, ["NO_FILES"]

        logger.info(
            f"Found PDFs - Annual: {annual_pdf.name if annual_pdf else 'None'}, "
            f"Quarterly: {quarterly_pdf.name if quarterly_pdf else 'None'}"
        )

        # Read PDF bytes
        annual_bytes = read_pdf_bytes(annual_pdf)
        if annual_bytes is None:
            logger.error(f"Could not read annual PDF for {company_name}")
            return False, ["READ_ERROR"]

        quarterly_bytes = None
        if quarterly_pdf:
            quarterly_bytes = read_pdf_bytes(quarterly_pdf)

        # Step 2: Threshold Check
        is_heavy, total_pages = is_heavy_report(annual_bytes)

        board_slice_bytes = None
        financial_slice_bytes = None

        if is_heavy:
            logger.warning(
                f"⚠️  HEAVY REPORT DETECTED ({total_pages} pages > {HEAVY_REPORT_THRESHOLD})"
            )
            logger.info("Engaging Smart Mapping Strategy with gemini-3-pro-preview...")

            # A. Run AI-powered structure mapper
            structure_map = map_report_structure(annual_bytes, model, annual_pdf.name)

            # B. Create targeted slices (returns bytes, not files)
            logger.info("Step 2b: Creating targeted PDF slices...")
            slices = create_report_slices(annual_bytes, structure_map)

            board_slice_bytes = slices.get('board_slice')
            financial_slice_bytes = slices.get('financial_slice')

            if not board_slice_bytes and not financial_slice_bytes:
                logger.warning("Could not create slices, falling back to standard processing")
                is_heavy = False
        else:
            logger.info(f"✅ Standard Report ({total_pages} pages). Using standard processing.")

        # Step 3: Upload PDFs to Gemini
        logger.info("Step 3: Uploading PDFs to Gemini...")

        full_annual_uri = None
        board_slice_uri = None
        financial_slice_uri = None
        quarterly_uri = None

        if is_heavy:
            # Upload slices
            if board_slice_bytes:
                board_slice_uri = upload_pdf_to_gemini(
                    board_slice_bytes,
                    f"board_slice_{annual_pdf.stem}.pdf"
                )
            if financial_slice_bytes:
                financial_slice_uri = upload_pdf_to_gemini(
                    financial_slice_bytes,
                    f"financial_slice_{annual_pdf.stem}.pdf"
                )

            if board_slice_uri is None and financial_slice_uri is None:
                logger.error(f"Failed to upload any slices for {company_name}")
                return False, ["UPLOAD_FAILED"]
        else:
            # Upload full annual report
            full_annual_uri = upload_pdf_to_gemini(annual_bytes, annual_pdf.name)
            if full_annual_uri is None:
                logger.error(f"Failed to upload annual report for {company_name}")
                return False, ["UPLOAD_FAILED"]

        # Upload quarterly if available
        if quarterly_bytes:
            quarterly_uri = upload_pdf_to_gemini(quarterly_bytes, quarterly_pdf.name)

        # Step 4: Generate sections
        logger.info("Step 4: Generating sections...")
        html_sections = []

        for section_id in SECTIONS:
            if is_heavy:
                # Route to appropriate slice based on section type
                if section_id in BOARD_REPORT_SECTIONS:
                    primary_uri = board_slice_uri or financial_slice_uri
                    logger.info(f"[Heavy → Board Slice] {section_id}")
                else:
                    primary_uri = financial_slice_uri or board_slice_uri
                    logger.info(f"[Heavy → Financial Slice] {section_id}")

                section_html = generate_section_with_fallback(
                    section_id=section_id,
                    primary_uri=primary_uri,
                    secondary_uri=quarterly_uri,
                    fallback_uri=primary_uri,
                    company_name=company_name
                )
            else:
                logger.info(f"[Standard] {section_id}")
                section_html = generate_section_with_fallback(
                    section_id=section_id,
                    primary_uri=full_annual_uri,
                    secondary_uri=quarterly_uri,
                    fallback_uri=full_annual_uri,
                    company_name=company_name
                )

            html_sections.append(section_html)

            if 'class="error"' in section_html:
                failed_sections.append(section_id)

            time.sleep(API_DELAY)

        # Step 5: Assemble and save HTML
        final_html = assemble_report(company_name, html_sections)

        output_company_dir = DEFAULT_OUTPUT_DIR / company_name
        html_output_file = output_company_dir / "final_report.html"
        pdf_output_file = output_company_dir / "final_report.pdf"

        if save_html_report(final_html, html_output_file):
            logger.info(f"HTML report saved to: {html_output_file}")
        else:
            logger.error(f"Failed to save HTML report for {company_name}")
            return False, ["SAVE_ERROR"]

        # Step 6: Convert to PDF
        logger.info("Step 6: Converting to PDF...")
        pdf_bytes = html_to_pdf(final_html)
        if pdf_bytes:
            if save_pdf_report(pdf_bytes, pdf_output_file):
                logger.info(f"PDF report saved to: {pdf_output_file}")
            else:
                logger.warning(f"Failed to save PDF for {company_name}")
        else:
            logger.warning(f"PDF conversion failed for {company_name}")

        if failed_sections:
            logger.warning(
                f"⚠️  {company_name}: {len(failed_sections)} section(s) FAILED: "
                f"{', '.join(failed_sections)}"
            )
            return False, failed_sections

        return True, []

    except Exception as e:
        logger.error(f"Unexpected error processing {company_name}: {e}")
        return False, ["EXCEPTION"]


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for batch report generation."""
    # Parse command line argument for company filter
    company_filter = sys.argv[1] if len(sys.argv) > 1 else None

    logger.info("=" * 60)
    logger.info("Starting Batch Financial Report Generator")
    logger.info(f"Model: {MODEL_NAME} (single model for all operations)")
    logger.info("=" * 60)

    # Validate configuration
    is_valid, errors = validate_config()
    if not is_valid:
        for error in errors:
            logger.error(error)
        return

    # Configure Gemini and get model instance
    try:
        model = configure_gemini()
    except Exception as e:
        logger.error(f"Failed to configure Gemini: {e}")
        return

    # Check input directory
    if not DEFAULT_FINANCIAL_REPORTS_DIR.exists():
        logger.error(f"Financial Reports directory not found: {DEFAULT_FINANCIAL_REPORTS_DIR}")
        return

    # Ensure output directory exists
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Find company directories
    company_dirs = [d for d in DEFAULT_FINANCIAL_REPORTS_DIR.iterdir() if d.is_dir()]

    # Apply company filter if provided
    if company_filter:
        company_dirs = [d for d in company_dirs if company_filter in d.name]
        if not company_dirs:
            logger.error(f"No company folder matching '{company_filter}' found")
            return
        logger.info(f"Filtered to companies matching: {company_filter}")

    if not company_dirs:
        logger.error("No company folders found in Financial_Reports directory")
        return

    logger.info(f"Found {len(company_dirs)} companies to process")

    # Process each company
    fully_successful = 0
    partial_success = 0
    total_failed = 0
    all_failures = {}

    for company_dir in sorted(company_dirs):
        try:
            success, failed_sections = process_company(company_dir, model)
            if success:
                fully_successful += 1
            elif failed_sections and failed_sections[0] not in ["NO_FILES", "UPLOAD_FAILED", "READ_ERROR", "SAVE_ERROR", "EXCEPTION"]:
                partial_success += 1
                all_failures[company_dir.name] = failed_sections
            else:
                total_failed += 1
                all_failures[company_dir.name] = failed_sections
        except Exception as e:
            logger.error(f"Unexpected error processing {company_dir.name}: {e}")
            total_failed += 1
            all_failures[company_dir.name] = ["EXCEPTION"]

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Fully successful (all sections): {fully_successful}")
    logger.info(f"Partial success (some sections failed): {partial_success}")
    logger.info(f"Failed (no report generated): {total_failed}")
    logger.info(f"Total companies: {fully_successful + partial_success + total_failed}")
    logger.info(f"Reports saved to: {DEFAULT_OUTPUT_DIR}")

    if all_failures:
        logger.error("\n" + "=" * 60)
        logger.error("⚠️  FAILURES REPORT - ACTION REQUIRED")
        logger.error("=" * 60)
        for company, sections in all_failures.items():
            logger.error(f"  {company}:")
            for section in sections:
                logger.error(f"    - {section}")
        logger.error("=" * 60)


if __name__ == "__main__":
    main()
