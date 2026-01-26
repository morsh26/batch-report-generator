"""
PDF Processor module for Financial Reports Service.

Handles all PyMuPDF (fitz) operations for PDF analysis and slicing.
This module is stateless - all functions accept bytes and return bytes/data.
"""

import json
import logging
from typing import Optional

import fitz  # PyMuPDF
import google.generativeai as genai

from .config import HEAVY_REPORT_THRESHOLD, TOC_SCAN_PAGES
from .ai_engine import generate_with_retry
from .prompts import get_structure_mapping_prompt

logger = logging.getLogger(__name__)


# =============================================================================
# PDF ANALYSIS FUNCTIONS (Stateless - work with bytes)
# =============================================================================

def get_pdf_page_count(pdf_bytes: bytes) -> int:
    """
    Get the total page count of a PDF from bytes.

    Args:
        pdf_bytes: PDF file content as bytes

    Returns:
        Number of pages in the PDF, or 0 if error
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        count = doc.page_count
        doc.close()
        return count
    except Exception as e:
        logger.error(f"Error getting page count: {e}")
        return 0


def is_heavy_report(pdf_bytes: bytes) -> tuple[bool, int]:
    """
    Check if a PDF is a heavy report (exceeds page threshold).

    Args:
        pdf_bytes: PDF file content as bytes

    Returns:
        Tuple of (is_heavy, total_pages)
    """
    total_pages = get_pdf_page_count(pdf_bytes)
    is_heavy = total_pages > HEAVY_REPORT_THRESHOLD
    return is_heavy, total_pages


def extract_toc_text(pdf_bytes: bytes, max_pages: int = TOC_SCAN_PAGES) -> str:
    """
    Extract text from the first N pages (Table of Contents area) using PyMuPDF.
    PyMuPDF is faster and handles Hebrew text better than PyPDF2.

    Args:
        pdf_bytes: PDF file content as bytes
        max_pages: Maximum number of pages to scan for TOC

    Returns:
        Extracted text from TOC pages
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        toc_text = []

        pages_to_scan = min(max_pages, doc.page_count)
        logger.info(f"  Extracting TOC text from first {pages_to_scan} pages...")

        for i in range(pages_to_scan):
            page = doc[i]
            text = page.get_text()
            if text:
                toc_text.append(f"--- Page {i + 1} ---\n{text}")

        doc.close()
        return "\n".join(toc_text)

    except Exception as e:
        logger.error(f"Error extracting TOC text: {e}")
        return ""


# =============================================================================
# PDF SLICING FUNCTIONS (Stateless - work with bytes)
# =============================================================================

def slice_pdf(
    pdf_bytes: bytes,
    start_page: int,
    end_page: int
) -> Optional[bytes]:
    """
    Create a new PDF containing only the specified page range.

    Args:
        pdf_bytes: Source PDF content as bytes
        start_page: Starting page index (0-indexed)
        end_page: Ending page index (0-indexed, inclusive)

    Returns:
        New PDF content as bytes, or None if error
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = doc.page_count

        start_page = max(0, start_page)
        end_page = min(total_pages - 1, end_page)

        if start_page > end_page:
            logger.error(f"Invalid page range: {start_page} to {end_page}")
            doc.close()
            return None

        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=start_page, to_page=end_page)

        # Get bytes from the new document
        pdf_output = new_doc.tobytes()

        new_doc.close()
        doc.close()

        pages_extracted = end_page - start_page + 1
        logger.info(f"    Created slice: pages {start_page + 1}-{end_page + 1} ({pages_extracted} pages)")

        return pdf_output

    except Exception as e:
        logger.error(f"Error slicing PDF: {e}")
        return None


# =============================================================================
# STRUCTURE MAPPING (AI-Powered)
# =============================================================================

def get_default_structure_map(total_pages: int) -> dict:
    """
    Return default page ranges based on typical Israeli financial report structure.

    Args:
        total_pages: Total number of pages in the PDF

    Returns:
        Dictionary with page ranges for each section (0-indexed)
    """
    logger.info(f"  Using default structure mapping for {total_pages} pages")

    board_end = int(total_pages * 0.25)
    financial_start = board_end
    financial_end = int(total_pages * 0.60)
    notes_start = financial_end

    structure = {
        'board_report': {'start': 0, 'end': board_end},
        'financial_statements': {'start': financial_start, 'end': financial_end},
        'notes': {'start': notes_start, 'end': total_pages - 1}
    }

    for section, ranges in structure.items():
        logger.info(f"    {section}: pages {ranges['start'] + 1}-{ranges['end'] + 1}")

    return structure


def map_report_structure(
    pdf_bytes: bytes,
    model: genai.GenerativeModel,
    filename: str = "report.pdf"
) -> dict:
    """
    Use AI (gemini-3-pro-preview) to analyze TOC and identify page ranges for report sections.
    Uses generate_with_retry for robust error handling.

    Args:
        pdf_bytes: PDF file content as bytes
        model: Gemini model instance
        filename: Original filename for logging

    Returns:
        Dictionary with page ranges (0-indexed):
        {
            'board_report': {'start': int, 'end': int},
            'financial_statements': {'start': int, 'end': int},
            'notes': {'start': int, 'end': int}
        }

        Falls back to default percentage-based ranges if AI mapping fails.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = doc.page_count
    doc.close()

    logger.info(f"  Running AI-powered structure mapping on {filename}...")

    # Extract TOC text from first pages
    toc_text = extract_toc_text(pdf_bytes, max_pages=TOC_SCAN_PAGES)

    if not toc_text:
        logger.warning("  Could not extract TOC text, using fallback ranges")
        return get_default_structure_map(total_pages)

    # Get the prompt for structure mapping
    prompt = get_structure_mapping_prompt(total_pages, toc_text)

    # Use retry wrapper for the API call
    response_text = generate_with_retry(
        prompt=prompt,
        model=model,
        operation_name="Structure Mapping"
    )

    if not response_text:
        logger.warning("  AI mapping returned no response, using fallback ranges")
        return get_default_structure_map(total_pages)

    # Parse JSON response
    try:
        # Clean up response - remove any markdown or extra text
        clean_response = response_text.strip()

        # Remove markdown code blocks if present
        if '```' in clean_response:
            parts = clean_response.split('```')
            for part in parts:
                part = part.strip()
                if part.startswith('json'):
                    part = part[4:].strip()
                if part.startswith('{') and part.endswith('}'):
                    clean_response = part
                    break

        # Find the JSON object in the response
        start_idx = clean_response.find('{')
        end_idx = clean_response.rfind('}') + 1
        if start_idx != -1 and end_idx > start_idx:
            clean_response = clean_response[start_idx:end_idx]

        structure_map = json.loads(clean_response)

        # Validate and convert to 0-indexed
        validated_map = {}
        for section in ['board_report', 'financial_statements', 'notes']:
            if section in structure_map:
                start = max(0, structure_map[section].get('start', 1) - 1)
                end = min(total_pages - 1, structure_map[section].get('end', total_pages) - 1)
                validated_map[section] = {'start': start, 'end': end}
                logger.info(f"    {section}: pages {start + 1}-{end + 1}")
            else:
                logger.warning(f"    {section}: not found in AI response")

        # Fill in any missing sections with defaults
        default_map = get_default_structure_map(total_pages)
        for section in ['board_report', 'financial_statements', 'notes']:
            if section not in validated_map:
                validated_map[section] = default_map[section]

        logger.info("  ✓ AI structure mapping completed")
        return validated_map

    except json.JSONDecodeError as e:
        logger.warning(f"  Failed to parse AI response as JSON: {e}")
        logger.warning(f"  Response was: {response_text[:300]}...")
        return get_default_structure_map(total_pages)
    except Exception as e:
        logger.warning(f"  AI mapping parsing failed: {e}")
        return get_default_structure_map(total_pages)


def create_report_slices(
    pdf_bytes: bytes,
    structure_map: dict
) -> dict[str, Optional[bytes]]:
    """
    Create sliced PDFs based on the structure map.

    Args:
        pdf_bytes: Source PDF content as bytes
        structure_map: Dictionary with page ranges

    Returns:
        Dictionary with slice names and their bytes:
        {
            'board_slice': bytes or None,
            'financial_slice': bytes or None
        }
    """
    slices = {}

    # Create board report slice
    board_range = structure_map.get('board_report', {})
    if board_range:
        slices['board_slice'] = slice_pdf(
            pdf_bytes, board_range['start'], board_range['end']
        )

    # Create financial slice (financial_statements + notes combined)
    fin_range = structure_map.get('financial_statements', {})
    notes_range = structure_map.get('notes', {})

    if fin_range and notes_range:
        start = fin_range['start']
        end = notes_range['end']
        slices['financial_slice'] = slice_pdf(pdf_bytes, start, end)
    elif fin_range:
        slices['financial_slice'] = slice_pdf(
            pdf_bytes, fin_range['start'], fin_range['end']
        )

    return slices
