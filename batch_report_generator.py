#!/usr/bin/env python3
"""
Batch Financial Report Generator

Automates generation of financial reports by:
1. Scanning Financial_Reports directory for company folders
2. Uploading PDFs to Google Gemini
3. Generating report sections via Supabase Edge Function
4. Assembling final HTML reports
"""

import os
import sys
import time
import logging
import tempfile
import re
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import requests
import google.generativeai as genai
from PyPDF2 import PdfReader, PdfWriter

# Load environment variables from .env file
load_dotenv()

# Optional: filter to specific company (pass as command line argument)
COMPANY_FILTER = sys.argv[1] if len(sys.argv) > 1 else None

# Configuration - paths can be overridden via environment variables
FINANCIAL_REPORTS_DIR = Path(os.getenv("FINANCIAL_REPORTS_DIR", "./Financial_Reports"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./All_Reports"))
SUPABASE_FUNCTION_URL = os.getenv("SUPABASE_FUNCTION_URL", "https://your-project.supabase.co/functions/v1/generate-compliance-report-v2")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# Report sections to generate (must match Edge Function's valid sectionIds)
SECTIONS = [
    'company_profile',
    'executive_summary',
    'business_environment',
    'asset_portfolio_analysis',
    'debt_structure',
    'financial_analysis',
    'cash_flow_and_liquidity',
    'liquidation_analysis'
]

# Section groups for hybrid token strategy
# Group A: Full context sections - need complete annual report for context
FULL_CONTEXT_SECTIONS = {
    'company_profile',
    'executive_summary',
    'business_environment',
    'asset_portfolio_analysis'
}

# Group B: Focused financial sections - use sliced financial statements only
FOCUSED_FINANCIAL_SECTIONS = {
    'debt_structure',
    'financial_analysis',
    'cash_flow_and_liquidity',
    'liquidation_analysis'
}

# Delay between API calls (seconds)
API_DELAY = 5.0  # Increased to avoid rate limits

# Rate limit retry settings
RATE_LIMIT_MAX_RETRIES = 5
RATE_LIMIT_BASE_DELAY = 30  # Start with 30 seconds wait on rate limit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_html_template(company_name: str) -> str:
    """Returns the HTML header template with RTL support and Hebrew fonts."""
    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>דוח פיננסי - {company_name}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Heebo:wght@300;400;500;700&display=swap');

        * {{
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Heebo', Arial, sans-serif;
            direction: rtl;
            text-align: right;
            line-height: 1.8;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 20px;
            background-color: #f5f5f5;
        }}

        .report-container {{
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}

        h1 {{
            color: #1a365d;
            border-bottom: 3px solid #2c5282;
            padding-bottom: 15px;
            margin-bottom: 30px;
        }}

        h2 {{
            color: #2c5282;
            margin-top: 40px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e2e8f0;
        }}

        h3 {{
            color: #4a5568;
            margin-top: 25px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}

        th, td {{
            border: 1px solid #e2e8f0;
            padding: 12px 15px;
            text-align: right;
        }}

        th {{
            background-color: #2c5282;
            color: white;
            font-weight: 500;
        }}

        tr:nth-child(even) {{
            background-color: #f7fafc;
        }}

        tr:hover {{
            background-color: #edf2f7;
        }}

        .section {{
            margin-bottom: 40px;
            padding: 20px;
            background: #fafafa;
            border-radius: 6px;
            border-right: 4px solid #2c5282;
        }}

        .error {{
            background-color: #fed7d7;
            color: #c53030;
            padding: 15px;
            border-radius: 6px;
            margin: 10px 0;
            border-right: 4px solid #c53030;
        }}

        .highlight {{
            background-color: #fefcbf;
            padding: 2px 6px;
            border-radius: 3px;
        }}

        ul, ol {{
            padding-right: 25px;
        }}

        li {{
            margin-bottom: 8px;
        }}

        .meta-info {{
            color: #718096;
            font-size: 12px;
            margin-bottom: 30px;
        }}
    </style>
</head>
<body>
    <div class="report-container">
        <h1>דוח פיננסי מקיף - {company_name}</h1>
        <div class="meta-info">
            נוצר באופן אוטומטי | תאריך יצירה: {time.strftime('%d/%m/%Y %H:%M')}
        </div>
"""


def get_html_footer() -> str:
    """Returns the HTML footer."""
    return """
    </div>
</body>
</html>
"""


def configure_gemini():
    """Configure Google Generative AI with API key."""
    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY environment variable is not set")
    genai.configure(api_key=GOOGLE_API_KEY)
    logger.info("Gemini API configured successfully")


# ============================================================
# PDF SLICING FUNCTIONS FOR HYBRID TOKEN STRATEGY
# ============================================================

def get_financial_range(pdf_path: Path) -> tuple[int, int]:
    """
    Identify the start and end pages of Financial Statements & Notes in the PDF.

    Searches for Hebrew/English markers that typically indicate:
    - Start: "דוחות כספיים", "Financial Statements", "דוח על המצב הכספי"
    - End: End of notes or start of appendices

    Returns (start_page, end_page) as 0-indexed page numbers.
    If not found, returns a reasonable default range (last 40% of document).
    """
    try:
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)

        # Markers for financial statements section (Hebrew and English)
        start_markers = [
            'דוחות כספיים',
            'דוח על המצב הכספי',
            'מאזן',
            'דוח רווח והפסד',
            'financial statements',
            'balance sheet',
            'statement of financial position',
            'consolidated statements'
        ]

        # Markers that indicate end of financial section
        end_markers = [
            'נספחים',
            'appendix',
            'פרטים נוספים',
            'דוח דירקטוריון',  # Usually comes after financials
            'פרק ה',  # Chapter markers
            'additional information'
        ]

        start_page = None
        end_page = total_pages - 1  # Default to last page

        logger.info(f"Scanning {pdf_path.name} ({total_pages} pages) for financial statements...")

        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text() or ""
                text_lower = text.lower()

                # Look for start markers
                if start_page is None:
                    for marker in start_markers:
                        if marker.lower() in text_lower:
                            start_page = i
                            logger.info(f"  Found financial section start at page {i + 1} (marker: '{marker}')")
                            break

                # Look for end markers (only after finding start)
                if start_page is not None and i > start_page + 10:  # At least 10 pages of financials
                    for marker in end_markers:
                        if marker.lower() in text_lower:
                            end_page = i - 1  # Page before the end marker
                            logger.info(f"  Found financial section end at page {end_page + 1} (marker: '{marker}')")
                            break
                    if end_page < total_pages - 1:
                        break  # Found end, stop scanning

            except Exception as e:
                logger.debug(f"  Could not extract text from page {i + 1}: {e}")
                continue

        # Fallback: if no markers found, use last 40% of document
        # (Financial statements are typically in the latter part of annual reports)
        if start_page is None:
            start_page = int(total_pages * 0.5)  # Start from 50%
            end_page = total_pages - 1
            logger.warning(f"  No financial markers found, using fallback range: pages {start_page + 1}-{end_page + 1}")

        # Ensure reasonable bounds
        start_page = max(0, start_page)
        end_page = min(total_pages - 1, end_page)

        # Ensure at least 20 pages (financial statements need context)
        if end_page - start_page < 20:
            end_page = min(start_page + 50, total_pages - 1)

        logger.info(f"  Financial range: pages {start_page + 1} to {end_page + 1} ({end_page - start_page + 1} pages)")
        return start_page, end_page

    except Exception as e:
        logger.error(f"Error analyzing PDF {pdf_path.name}: {e}")
        # Return last 40% as safe fallback
        try:
            reader = PdfReader(str(pdf_path))
            total_pages = len(reader.pages)
            return int(total_pages * 0.5), total_pages - 1
        except:
            return 0, 100  # Absolute fallback


def slice_pdf_pages(pdf_path: Path, start_page: int, end_page: int, output_dir: Path) -> Optional[Path]:
    """
    Create a new PDF containing only the specified page range.

    Args:
        pdf_path: Path to the source PDF
        start_page: Start page (0-indexed)
        end_page: End page (0-indexed, inclusive)
        output_dir: Directory to save the sliced PDF

    Returns:
        Path to the sliced PDF, or None if slicing fails
    """
    try:
        reader = PdfReader(str(pdf_path))
        writer = PdfWriter()

        total_pages = len(reader.pages)

        # Validate page range
        start_page = max(0, start_page)
        end_page = min(total_pages - 1, end_page)

        if start_page > end_page:
            logger.error(f"Invalid page range: {start_page} to {end_page}")
            return None

        # Extract pages
        for i in range(start_page, end_page + 1):
            writer.add_page(reader.pages[i])

        # Create output filename
        output_filename = f"sliced_financials_{pdf_path.stem}.pdf"
        output_path = output_dir / output_filename

        # Write sliced PDF
        with open(output_path, 'wb') as f:
            writer.write(f)

        pages_extracted = end_page - start_page + 1
        logger.info(f"Created sliced PDF: {output_path.name} ({pages_extracted} pages from {total_pages} total)")

        return output_path

    except Exception as e:
        logger.error(f"Error slicing PDF {pdf_path.name}: {e}")
        return None


def create_sliced_annual_pdf(annual_pdf: Path, company_dir: Path) -> Optional[Path]:
    """
    Create a sliced version of the annual PDF containing only financial statements.

    Args:
        annual_pdf: Path to the full annual report PDF
        company_dir: Company directory (used for temp storage)

    Returns:
        Path to the sliced PDF, or None if creation fails
    """
    logger.info(f"Creating sliced financial PDF from {annual_pdf.name}...")

    # Get the page range for financial statements
    start_page, end_page = get_financial_range(annual_pdf)

    # Create the sliced PDF in the company directory
    sliced_path = slice_pdf_pages(annual_pdf, start_page, end_page, company_dir)

    return sliced_path


def find_pdf_files(company_dir: Path) -> tuple[Optional[Path], Optional[Path]]:
    """
    Find Annual and Quarterly PDF files in a company directory.
    Returns (annual_pdf, quarterly_pdf) - quarterly may be None.
    """
    annual_pdf = None
    quarterly_pdf = None

    for file in company_dir.glob("*.pdf"):
        filename = file.name.lower()
        if "annual" in filename or "שנתי" in filename:
            annual_pdf = file
        elif any(q in filename for q in ["quarter", "רבעוני", "q1", "q2", "q3", "q4"]):
            quarterly_pdf = file

    # If no specific match, take any PDFs found (sorted by name for consistency)
    if annual_pdf is None:
        pdfs = sorted(company_dir.glob("*.pdf"))
        if pdfs:
            annual_pdf = pdfs[0]
            if len(pdfs) > 1:
                quarterly_pdf = pdfs[1]

    return annual_pdf, quarterly_pdf


def upload_pdf_to_gemini(pdf_path: Path, max_retries: int = 3) -> Optional[str]:
    """
    Upload a PDF file to Gemini and wait for it to become ACTIVE.
    Returns the file URI or None if upload fails.
    """
    logger.info(f"Uploading {pdf_path.name} to Gemini...")

    for attempt in range(max_retries):
        try:
            # Upload the file
            uploaded_file = genai.upload_file(
                path=str(pdf_path),
                display_name=pdf_path.name
            )

            # Wait for file to be processed
            logger.info(f"Waiting for {pdf_path.name} to be processed...")
            while uploaded_file.state.name == "PROCESSING":
                time.sleep(2)
                uploaded_file = genai.get_file(uploaded_file.name)

            if uploaded_file.state.name == "ACTIVE":
                logger.info(f"Successfully uploaded {pdf_path.name}: {uploaded_file.uri}")
                return uploaded_file.uri
            else:
                logger.error(f"File {pdf_path.name} failed to process. State: {uploaded_file.state.name}")
                return None

        except Exception as e:
            logger.warning(f"Upload attempt {attempt + 1} failed for {pdf_path.name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                logger.error(f"Failed to upload {pdf_path.name} after {max_retries} attempts")
                return None

    return None


def is_token_limit_error(response: requests.Response) -> bool:
    """Check if the error is related to token limits."""
    if response.status_code == 400:
        return True

    error_text = response.text.lower()
    token_limit_keywords = [
        'token', 'limit', 'exceed', 'invalidargument',
        'resourceexhausted', 'too large', 'context length'
    ]
    return any(keyword in error_text for keyword in token_limit_keywords)


def is_rate_limit_error(response: requests.Response) -> bool:
    """Check if the error is a rate limit (429) error."""
    if response.status_code == 429:
        return True

    error_text = response.text.lower()
    rate_limit_keywords = ['too many requests', 'rate limit', 'quota', '429']
    return any(keyword in error_text for keyword in rate_limit_keywords)


def call_section_api(
    section_id: str,
    file_uri1: str,
    file_uri2: Optional[str],
    company_name: str,
    display_name: str
) -> tuple[Optional[str], bool]:
    """
    Make API call to generate a section.
    Returns (html_content, is_token_error).
    html_content is None if failed, string if successful.
    is_token_error indicates if failure was due to token limits.

    Handles rate limits (429) with exponential backoff.
    """
    payload = {
        "action": "generate_section",
        "sectionId": section_id,
        "fileUri1": file_uri1,
        "fileUri2": file_uri2 or "",
        "companyName": company_name,
        "model": "gemini-3-pro-preview"
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}"
    }

    rate_limit_retries = 0
    general_retries = 0
    max_general_retries = 3

    while True:
        try:
            response = requests.post(
                SUPABASE_FUNCTION_URL,
                json=payload,
                headers=headers,
                timeout=180
            )

            if response.status_code == 200:
                data = response.json()
                html_content = data.get("html", data.get("content", ""))
                if html_content:
                    return html_content, False
                else:
                    return None, False

            # Check for rate limit error (429) - use exponential backoff
            if is_rate_limit_error(response):
                rate_limit_retries += 1
                if rate_limit_retries <= RATE_LIMIT_MAX_RETRIES:
                    wait_time = RATE_LIMIT_BASE_DELAY * (2 ** (rate_limit_retries - 1))  # 30, 60, 120, 240, 480 seconds
                    logger.warning(f"⏳ Rate limit hit for {display_name}. Waiting {wait_time}s before retry {rate_limit_retries}/{RATE_LIMIT_MAX_RETRIES}...")
                    time.sleep(wait_time)
                    continue  # Retry without incrementing general_retries
                else:
                    logger.error(f"Rate limit exceeded max retries for {display_name}")
                    return None, False

            # Check for token limit error
            if is_token_limit_error(response):
                return None, True

            # Other 500 errors - retry with general counter
            if response.status_code == 500:
                general_retries += 1
                if general_retries < max_general_retries:
                    logger.warning(f"Attempt {general_retries} failed for {display_name} (500 error), retrying in 10s...")
                    time.sleep(10)
                    continue
                else:
                    logger.error(f"API error for {display_name}: {response.status_code} - {response.text[:200]}")
                    return None, False

            # Other errors - don't retry
            logger.error(f"API error for {display_name}: {response.status_code} - {response.text[:200]}")
            return None, False

        except requests.exceptions.Timeout:
            general_retries += 1
            if general_retries < max_general_retries:
                logger.warning(f"Timeout on attempt {general_retries} for {display_name}, retrying in 10s...")
                time.sleep(10)
                continue
            logger.error(f"Timeout for {display_name} after {general_retries} attempts")
            return None, False
        except Exception as e:
            logger.error(f"Exception generating {display_name}: {e}")
            return None, False


def generate_section_with_fallback(
    section_id: str,
    primary_uri: str,
    secondary_uri: Optional[str],
    fallback_uri: str,
    company_name: str
) -> str:
    """
    Generate a report section with smart fallback for token limit errors.

    Strategy:
    - Phase 1: Try with [primary_uri, secondary_uri]
    - Phase 2: If token limit error, retry with [fallback_uri] only
    - Phase 3: If still fails, return error HTML (don't crash)

    Args:
        section_id: The section identifier
        primary_uri: Main file URI (full annual or sliced annual)
        secondary_uri: Secondary file URI (quarterly, can be None)
        fallback_uri: Fallback file URI if token limit hit
        company_name: Company name for the report

    Returns HTML snippet or error HTML.
    """
    section_display_names = {
        'company_profile': 'פרופיל חברה',
        'executive_summary': 'תקציר מנהלים',
        'business_environment': 'סביבה עסקית',
        'asset_portfolio_analysis': 'ניתוח תיק נכסים',
        'debt_structure': 'מבנה חוב',
        'financial_analysis': 'ניתוח פיננסי',
        'cash_flow_and_liquidity': 'תזרים מזומנים ונזילות',
        'liquidation_analysis': 'ניתוח פירוק'
    }

    display_name = section_display_names.get(section_id, section_id)
    logger.info(f"  Generating: {display_name} ({section_id})...")

    # ============================================================
    # PHASE 1: Try with primary + secondary files
    # ============================================================
    files_desc = "primary + secondary" if secondary_uri else "primary only"
    logger.info(f"    Phase 1: Attempting with {files_desc}...")

    html_content, is_token_error = call_section_api(
        section_id, primary_uri, secondary_uri, company_name, display_name
    )

    if html_content:
        logger.info(f"    ✓ Successfully generated {display_name}")
        return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

    # ============================================================
    # PHASE 2: Fallback to single file if token limit error
    # ============================================================
    if is_token_error:
        logger.warning(f"    ⚠️ Token Limit Hit - Retrying with fallback file only...")

        html_content, is_token_error_2 = call_section_api(
            section_id, fallback_uri, None, company_name, display_name
        )

        if html_content:
            logger.info(f"    ✓ Successfully generated {display_name} (fallback)")
            return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

        # ============================================================
        # PHASE 3: Final failure
        # ============================================================
        if is_token_error_2:
            logger.error(f"    ❌ {display_name} failed - token limit exceeded even with single file")
            return f'<div class="error">שגיאה: {display_name} - הקובץ גדול מדי גם עם קובץ בודד</div>'
        else:
            logger.error(f"    ❌ {display_name} failed on fallback")
            return f'<div class="error">שגיאה בייצור {display_name} (fallback נכשל)</div>'

    # Non-token-limit error in phase 1
    logger.error(f"    ❌ {display_name} failed (not a token limit error)")
    return f'<div class="error">שגיאה בייצור {display_name}</div>'


def process_company(company_dir: Path) -> tuple[bool, list[str]]:
    """
    Process a single company using HYBRID TOKEN STRATEGY:

    1. Pre-process: Create sliced PDF with only financial statements
    2. Upload: Full annual, sliced annual, and quarterly PDFs
    3. Generate sections:
       - Group A (Full Context): Use [full_annual, quarterly] with fallback
       - Group B (Focused Financial): Use [sliced_annual, quarterly] always

    Returns (success, failed_sections) - success is True only if ALL sections generated successfully.
    """
    company_name = company_dir.name
    failed_sections = []
    sliced_pdf_path = None  # Track for cleanup

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing company: {company_name}")
    logger.info(f"{'='*60}")

    try:
        # ============================================================
        # STEP 1: Find PDF files
        # ============================================================
        annual_pdf, quarterly_pdf = find_pdf_files(company_dir)

        if annual_pdf is None:
            logger.error(f"No PDF files found for {company_name}")
            return False, ["NO_FILES"]

        logger.info(f"Found PDFs - Annual: {annual_pdf.name if annual_pdf else 'None'}, "
                    f"Quarterly: {quarterly_pdf.name if quarterly_pdf else 'None'}")

        # ============================================================
        # STEP 2: PRE-PROCESS - Create sliced financial PDF
        # ============================================================
        logger.info("Step 2: Creating sliced financial PDF...")
        sliced_pdf_path = create_sliced_annual_pdf(annual_pdf, company_dir)

        if sliced_pdf_path is None:
            logger.warning("Could not create sliced PDF, will use full annual for all sections")

        # ============================================================
        # STEP 3: Upload all PDFs to Gemini
        # ============================================================
        logger.info("Step 3: Uploading PDFs to Gemini...")

        # Upload full annual PDF
        full_annual_uri = upload_pdf_to_gemini(annual_pdf)
        if full_annual_uri is None:
            logger.error(f"Failed to upload annual report for {company_name}")
            return False, ["UPLOAD_FAILED"]

        # Upload sliced annual PDF (if created)
        sliced_annual_uri = None
        if sliced_pdf_path:
            sliced_annual_uri = upload_pdf_to_gemini(sliced_pdf_path)
            if sliced_annual_uri is None:
                logger.warning("Failed to upload sliced PDF, will use full annual for financial sections")

        # Upload quarterly PDF (if exists)
        quarterly_uri = None
        if quarterly_pdf:
            quarterly_uri = upload_pdf_to_gemini(quarterly_pdf)
            if quarterly_uri is None:
                logger.warning(f"Failed to upload quarterly report for {company_name}, continuing without it")

        # ============================================================
        # STEP 4: Generate sections using HYBRID STRATEGY
        # ============================================================
        logger.info("Step 4: Generating sections with hybrid token strategy...")
        html_sections = []

        for section_id in SECTIONS:
            # Determine which files to use based on section group
            if section_id in FULL_CONTEXT_SECTIONS:
                # GROUP A: Full Context Sections
                # Strategy: Try [full_annual, quarterly], fallback to [full_annual] only
                logger.info(f"[Group A - Full Context] {section_id}")
                section_html = generate_section_with_fallback(
                    section_id=section_id,
                    primary_uri=full_annual_uri,
                    secondary_uri=quarterly_uri,
                    fallback_uri=full_annual_uri,  # Fallback: annual only
                    company_name=company_name
                )
            else:
                # GROUP B: Focused Financial Sections
                # Strategy: ALWAYS use [sliced_annual, quarterly] to save tokens
                logger.info(f"[Group B - Focused Financial] {section_id}")

                # Use sliced if available, otherwise full annual
                financial_uri = sliced_annual_uri if sliced_annual_uri else full_annual_uri

                section_html = generate_section_with_fallback(
                    section_id=section_id,
                    primary_uri=financial_uri,
                    secondary_uri=quarterly_uri,
                    fallback_uri=financial_uri,  # Fallback: sliced/annual only
                    company_name=company_name
                )

            html_sections.append(section_html)

            # Track failed sections
            if 'class="error"' in section_html:
                failed_sections.append(section_id)

            time.sleep(API_DELAY)  # Rate limiting delay

        # ============================================================
        # STEP 5: Assemble and save final HTML
        # ============================================================
        final_html = get_html_template(company_name)
        final_html += "\n".join(html_sections)
        final_html += get_html_footer()

        # Save report
        output_company_dir = OUTPUT_DIR / company_name
        output_company_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_company_dir / "final_report.html"

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_html)

        logger.info(f"Report saved to: {output_file}")

        if failed_sections:
            logger.warning(f"⚠️  {company_name}: {len(failed_sections)} section(s) FAILED: {', '.join(failed_sections)}")
            return False, failed_sections

        return True, []

    finally:
        # Cleanup: Remove temporary sliced PDF
        if sliced_pdf_path and sliced_pdf_path.exists():
            try:
                sliced_pdf_path.unlink()
                logger.debug(f"Cleaned up temporary file: {sliced_pdf_path}")
            except Exception as e:
                logger.warning(f"Could not delete temporary file {sliced_pdf_path}: {e}")


def main():
    """Main entry point for batch report generation."""
    logger.info("=" * 60)
    logger.info("Starting Batch Financial Report Generator")
    logger.info("=" * 60)

    # Validate configuration
    if not SUPABASE_ANON_KEY:
        logger.error("SUPABASE_ANON_KEY environment variable is not set")
        return

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY environment variable is not set")
        return

    # Configure Gemini
    try:
        configure_gemini()
    except Exception as e:
        logger.error(f"Failed to configure Gemini: {e}")
        return

    # Verify input directory exists
    if not FINANCIAL_REPORTS_DIR.exists():
        logger.error(f"Financial Reports directory not found: {FINANCIAL_REPORTS_DIR}")
        return

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get all company directories
    company_dirs = [d for d in FINANCIAL_REPORTS_DIR.iterdir() if d.is_dir()]

    # Filter by company name if specified
    if COMPANY_FILTER:
        company_dirs = [d for d in company_dirs if COMPANY_FILTER in d.name]
        if not company_dirs:
            logger.error(f"No company folder matching '{COMPANY_FILTER}' found")
            return
        logger.info(f"Filtered to companies matching: {COMPANY_FILTER}")

    if not company_dirs:
        logger.error("No company folders found in Financial_Reports directory")
        return

    logger.info(f"Found {len(company_dirs)} companies to process")

    # Process each company
    fully_successful = 0
    partial_success = 0
    total_failed = 0
    all_failures = {}  # company -> failed sections

    for company_dir in sorted(company_dirs):
        try:
            success, failed_sections = process_company(company_dir)
            if success:
                fully_successful += 1
            elif failed_sections and failed_sections[0] not in ["NO_FILES", "UPLOAD_FAILED"]:
                # Partial success - report generated but some sections failed
                partial_success += 1
                all_failures[company_dir.name] = failed_sections
            else:
                # Total failure - couldn't even generate report
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
    logger.info(f"Reports saved to: {OUTPUT_DIR}")

    # Show detailed failure report
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
