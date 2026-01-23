#!/usr/bin/env python3
"""
Batch Financial Report Generator

Automates generation of financial reports using Smart Threshold & Dynamic Mapping Strategy.
Uses ONLY gemini-3-pro-preview for all AI operations with robust exponential backoff retry logic.

Key Features:
- Heavy report detection (>300 pages)
- AI-powered structure mapping for TOC analysis
- Targeted PDF slicing for token optimization
- Exponential backoff for rate limit handling
"""

import os
import sys
import time
import logging
import json
from pathlib import Path
from typing import Optional, Callable, Any

from dotenv import load_dotenv
import requests
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import fitz  # PyMuPDF - faster and better with Hebrew than PyPDF2

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

# ============================================================
# MODEL CONFIGURATION - SINGLE MODEL FOR EVERYTHING
# ============================================================
MODEL_NAME = "gemini-3-pro-preview"

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

# Section routing for heavy reports
BOARD_REPORT_SECTIONS = {
    'company_profile',
    'executive_summary',
    'business_environment',
    'asset_portfolio_analysis'
}

FINANCIAL_STATEMENTS_SECTIONS = {
    'debt_structure',
    'financial_analysis',
    'cash_flow_and_liquidity',
    'liquidation_analysis'
}

# Delay between API calls (seconds)
API_DELAY = 5.0

# ============================================================
# RETRY CONFIGURATION - EXPONENTIAL BACKOFF FOR ALL API CALLS
# ============================================================
MAX_RETRIES = 6
BASE_DELAY = 30  # Starting delay in seconds
MAX_DELAY = 600  # Maximum delay (10 minutes)

# Heavy report threshold (pages)
HEAVY_REPORT_THRESHOLD = 300

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global model instance - initialized once
gemini_model: Optional[genai.GenerativeModel] = None


# ============================================================
# CORE RETRY WRAPPER - USED FOR ALL GEMINI API CALLS
# ============================================================

def generate_with_retry(
    prompt: str,
    model: genai.GenerativeModel,
    operation_name: str = "API call",
    max_retries: int = MAX_RETRIES,
    base_delay: int = BASE_DELAY
) -> Optional[str]:
    """
    Execute a Gemini API call with exponential backoff retry logic.

    Handles:
    - 429 Resource Exhausted (rate limits)
    - 500/503 Server errors
    - Timeout errors
    - Other transient errors

    Args:
        prompt: The prompt to send to the model
        model: The Gemini model instance
        operation_name: Description of the operation for logging
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff

    Returns:
        Response text or None if all retries failed
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            logger.debug(f"  {operation_name}: Attempt {attempt + 1}/{max_retries}")

            response = model.generate_content(prompt)

            if response and response.text:
                return response.text.strip()
            else:
                logger.warning(f"  {operation_name}: Empty response on attempt {attempt + 1}")
                last_error = "Empty response"

        except google_exceptions.ResourceExhausted as e:
            # Rate limit - use exponential backoff
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(f"⏳ Rate limit hit for {operation_name}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except google_exceptions.ServiceUnavailable as e:
            # Server overloaded - wait and retry
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(f"⏳ Service unavailable for {operation_name}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except google_exceptions.DeadlineExceeded as e:
            # Timeout - wait and retry
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(f"⏳ Timeout for {operation_name}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except Exception as e:
            error_str = str(e).lower()

            # Check if it's a rate limit error in the message
            if '429' in error_str or 'resource' in error_str or 'exhausted' in error_str or 'quota' in error_str:
                wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
                logger.warning(f"⏳ Rate limit (from error message) for {operation_name}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                last_error = str(e)
                continue

            # Other errors - log and retry with shorter delay
            logger.warning(f"  {operation_name}: Error on attempt {attempt + 1}: {e}")
            last_error = str(e)

            if attempt < max_retries - 1:
                wait_time = min(10 * (attempt + 1), 60)
                time.sleep(wait_time)

    logger.error(f"❌ {operation_name} failed after {max_retries} attempts. Last error: {last_error}")
    return None


# ============================================================
# INITIALIZATION
# ============================================================

def configure_gemini() -> genai.GenerativeModel:
    """Configure Google Generative AI and return the model instance."""
    global gemini_model

    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY environment variable is not set")

    genai.configure(api_key=GOOGLE_API_KEY)

    # Initialize the single model instance
    gemini_model = genai.GenerativeModel(MODEL_NAME)

    logger.info(f"Gemini API configured successfully with model: {MODEL_NAME}")
    return gemini_model


# ============================================================
# HTML TEMPLATE
# ============================================================

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


# ============================================================
# PDF HANDLING WITH PYMUPDF (FITZ)
# ============================================================

def get_pdf_page_count(pdf_path: Path) -> int:
    """Get the total page count of a PDF using PyMuPDF."""
    try:
        doc = fitz.open(str(pdf_path))
        count = doc.page_count
        doc.close()
        return count
    except Exception as e:
        logger.error(f"Error getting page count for {pdf_path.name}: {e}")
        return 0


def extract_toc_text(pdf_path: Path, max_pages: int = 30) -> str:
    """
    Extract text from the first N pages (Table of Contents area) using PyMuPDF.
    PyMuPDF is faster and handles Hebrew text better than PyPDF2.
    """
    try:
        doc = fitz.open(str(pdf_path))
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
        logger.error(f"Error extracting TOC text from {pdf_path.name}: {e}")
        return ""


def map_report_structure(pdf_path: Path, model: genai.GenerativeModel) -> dict:
    """
    Use AI (gemini-3-pro-preview) to analyze TOC and identify page ranges for report sections.
    Uses generate_with_retry for robust error handling.

    Returns a dictionary with page ranges:
    {
        'board_report': {'start': int, 'end': int},
        'financial_statements': {'start': int, 'end': int},
        'notes': {'start': int, 'end': int}
    }

    Falls back to default percentage-based ranges if AI mapping fails.
    """
    doc = fitz.open(str(pdf_path))
    total_pages = doc.page_count
    doc.close()

    logger.info(f"  Running AI-powered structure mapping on {pdf_path.name}...")

    # Extract TOC text from first 30 pages
    toc_text = extract_toc_text(pdf_path, max_pages=30)

    if not toc_text:
        logger.warning("  Could not extract TOC text, using fallback ranges")
        return get_default_structure_map(total_pages)

    # Strict prompt for JSON output
    prompt = f"""You are analyzing the Table of Contents of an Israeli financial report.
The report has {total_pages} total pages.

IMPORTANT: Return ONLY a valid JSON object. No markdown, no explanation, no code blocks.

Your response must be exactly in this format (replace numbers with actual page numbers):
{{"board_report": {{"start": 1, "end": 100}}, "financial_statements": {{"start": 101, "end": 200}}, "notes": {{"start": 201, "end": 300}}}}

Section definitions:
- board_report: דוח דירקטוריון / תיאור עסקי התאגיד / פרק א' (company description, business overview, management discussion)
- financial_statements: דוחות כספיים / פרק ג' (consolidated balance sheet, income statement, cash flow statement)
- notes: ביאורים לדוחות הכספיים (notes to financial statements, accounting policies)

Page numbers should be 1-indexed (first page = 1).
Analyze carefully and provide accurate page ranges based on the TOC below.

TOC TEXT:
{toc_text[:12000]}"""

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
            # Extract content between code blocks
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


def get_default_structure_map(total_pages: int) -> dict:
    """
    Return default page ranges based on typical Israeli financial report structure.
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


def slice_pdf_fitz(pdf_path: Path, start_page: int, end_page: int, output_path: Path) -> Optional[Path]:
    """Create a new PDF containing only the specified page range using PyMuPDF."""
    try:
        doc = fitz.open(str(pdf_path))
        total_pages = doc.page_count

        start_page = max(0, start_page)
        end_page = min(total_pages - 1, end_page)

        if start_page > end_page:
            logger.error(f"Invalid page range: {start_page} to {end_page}")
            doc.close()
            return None

        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=start_page, to_page=end_page)

        new_doc.save(str(output_path))
        new_doc.close()
        doc.close()

        pages_extracted = end_page - start_page + 1
        logger.info(f"    Created slice: {output_path.name} ({pages_extracted} pages)")

        return output_path

    except Exception as e:
        logger.error(f"Error slicing PDF {pdf_path.name}: {e}")
        return None


def create_report_slices(pdf_path: Path, structure_map: dict, output_dir: Path) -> dict:
    """Create sliced PDFs based on the structure map."""
    slices = {}

    # Create board report slice
    board_range = structure_map.get('board_report', {})
    if board_range:
        board_path = output_dir / f"slice_board_{pdf_path.stem}.pdf"
        slices['board_slice'] = slice_pdf_fitz(
            pdf_path, board_range['start'], board_range['end'], board_path
        )

    # Create financial slice (financial_statements + notes combined)
    fin_range = structure_map.get('financial_statements', {})
    notes_range = structure_map.get('notes', {})

    if fin_range and notes_range:
        start = fin_range['start']
        end = notes_range['end']
        financial_path = output_dir / f"slice_financial_{pdf_path.stem}.pdf"
        slices['financial_slice'] = slice_pdf_fitz(pdf_path, start, end, financial_path)
    elif fin_range:
        financial_path = output_dir / f"slice_financial_{pdf_path.stem}.pdf"
        slices['financial_slice'] = slice_pdf_fitz(
            pdf_path, fin_range['start'], fin_range['end'], financial_path
        )

    return slices


def is_heavy_report(pdf_path: Path) -> tuple[bool, int]:
    """Check if a PDF is a heavy report (exceeds page threshold)."""
    total_pages = get_pdf_page_count(pdf_path)
    is_heavy = total_pages > HEAVY_REPORT_THRESHOLD
    return is_heavy, total_pages


def find_pdf_files(company_dir: Path) -> tuple[Optional[Path], Optional[Path]]:
    """Find Annual and Quarterly PDF files in a company directory."""
    annual_pdf = None
    quarterly_pdf = None

    for file in company_dir.glob("*.pdf"):
        filename = file.name.lower()
        if "annual" in filename or "שנתי" in filename:
            annual_pdf = file
        elif any(q in filename for q in ["quarter", "רבעוני", "q1", "q2", "q3", "q4"]):
            quarterly_pdf = file

    if annual_pdf is None:
        pdfs = sorted(company_dir.glob("*.pdf"))
        if pdfs:
            annual_pdf = pdfs[0]
            if len(pdfs) > 1:
                quarterly_pdf = pdfs[1]

    return annual_pdf, quarterly_pdf


def upload_pdf_to_gemini(pdf_path: Path, max_retries: int = 5) -> Optional[str]:
    """Upload a PDF file to Gemini with retry logic."""
    logger.info(f"Uploading {pdf_path.name} to Gemini...")

    for attempt in range(max_retries):
        try:
            uploaded_file = genai.upload_file(
                path=str(pdf_path),
                display_name=pdf_path.name
            )

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
            error_str = str(e).lower()
            if '429' in error_str or 'resource' in error_str or 'exhausted' in error_str:
                wait_time = BASE_DELAY * (2 ** attempt)
                logger.warning(f"⏳ Rate limit on upload. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue

            logger.warning(f"Upload attempt {attempt + 1} failed for {pdf_path.name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                logger.error(f"Failed to upload {pdf_path.name} after {max_retries} attempts")
                return None

    return None


# ============================================================
# SECTION GENERATION VIA SUPABASE EDGE FUNCTION
# ============================================================

def is_token_limit_error(response: requests.Response) -> bool:
    """Check if the error is related to token limits."""
    if response.status_code == 400:
        return True
    error_text = response.text.lower()
    token_limit_keywords = ['token', 'limit', 'exceed', 'invalidargument', 'resourceexhausted', 'too large', 'context length']
    return any(keyword in error_text for keyword in token_limit_keywords)


def is_rate_limit_error(response: requests.Response) -> bool:
    """Check if the error is a rate limit (429) error."""
    if response.status_code == 429:
        return True
    error_text = response.text.lower()
    rate_limit_keywords = ['too many requests', 'rate limit', 'quota', '429', 'resource exhausted']
    return any(keyword in error_text for keyword in rate_limit_keywords)


def call_section_api(
    section_id: str,
    file_uri1: str,
    file_uri2: Optional[str],
    company_name: str,
    display_name: str
) -> tuple[Optional[str], bool]:
    """
    Make API call to generate a section with exponential backoff retry.
    Returns (html_content, is_token_error).
    """
    payload = {
        "action": "generate_section",
        "sectionId": section_id,
        "fileUri1": file_uri1,
        "fileUri2": file_uri2 or "",
        "companyName": company_name,
        "model": MODEL_NAME
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
                timeout=300  # 5 minute timeout for heavy operations
            )

            if response.status_code == 200:
                data = response.json()
                html_content = data.get("html", data.get("content", ""))
                if html_content:
                    return html_content, False
                else:
                    return None, False

            # Rate limit - exponential backoff
            if is_rate_limit_error(response):
                rate_limit_retries += 1
                if rate_limit_retries <= MAX_RETRIES:
                    wait_time = min(BASE_DELAY * (2 ** (rate_limit_retries - 1)), MAX_DELAY)
                    logger.warning(f"⏳ Rate limit hit for {display_name}. Waiting {wait_time}s before retry {rate_limit_retries}/{MAX_RETRIES}...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Rate limit exceeded max retries for {display_name}")
                    return None, False

            # Token limit error
            if is_token_limit_error(response):
                return None, True

            # Other 500 errors
            if response.status_code == 500:
                general_retries += 1
                if general_retries < max_general_retries:
                    wait_time = 10 * general_retries
                    logger.warning(f"Attempt {general_retries} failed for {display_name} (500 error), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API error for {display_name}: {response.status_code} - {response.text[:200]}")
                    return None, False

            logger.error(f"API error for {display_name}: {response.status_code} - {response.text[:200]}")
            return None, False

        except requests.exceptions.Timeout:
            general_retries += 1
            if general_retries < max_general_retries:
                logger.warning(f"Timeout on attempt {general_retries} for {display_name}, retrying...")
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
    """Generate a report section with smart fallback for token limit errors."""
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

    # Phase 1: Try with primary + secondary files
    files_desc = "primary + secondary" if secondary_uri else "primary only"
    logger.info(f"    Phase 1: Attempting with {files_desc}...")

    html_content, is_token_error = call_section_api(
        section_id, primary_uri, secondary_uri, company_name, display_name
    )

    if html_content:
        logger.info(f"    ✓ Successfully generated {display_name}")
        return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

    # Phase 2: Fallback to single file if token limit error
    if is_token_error:
        logger.warning(f"    ⚠️ Token Limit Hit - Retrying with fallback file only...")

        html_content, is_token_error_2 = call_section_api(
            section_id, fallback_uri, None, company_name, display_name
        )

        if html_content:
            logger.info(f"    ✓ Successfully generated {display_name} (fallback)")
            return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

        if is_token_error_2:
            logger.error(f"    ❌ {display_name} failed - token limit exceeded even with single file")
            return f'<div class="error">שגיאה: {display_name} - הקובץ גדול מדי גם עם קובץ בודד</div>'
        else:
            logger.error(f"    ❌ {display_name} failed on fallback")
            return f'<div class="error">שגיאה בייצור {display_name} (fallback נכשל)</div>'

    logger.error(f"    ❌ {display_name} failed (not a token limit error)")
    return f'<div class="error">שגיאה בייצור {display_name}</div>'


# ============================================================
# MAIN PROCESSING LOGIC
# ============================================================

def process_company(company_dir: Path, model: genai.GenerativeModel) -> tuple[bool, list[str]]:
    """
    Process a single company using Smart Threshold & Dynamic Mapping Strategy.
    Uses gemini-3-pro-preview for ALL AI operations.
    """
    company_name = company_dir.name
    failed_sections = []
    temp_files = []

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing company: {company_name}")
    logger.info(f"{'='*60}")

    try:
        # Step 1: Find PDF files
        annual_pdf, quarterly_pdf = find_pdf_files(company_dir)

        if annual_pdf is None:
            logger.error(f"No PDF files found for {company_name}")
            return False, ["NO_FILES"]

        logger.info(f"Found PDFs - Annual: {annual_pdf.name if annual_pdf else 'None'}, "
                    f"Quarterly: {quarterly_pdf.name if quarterly_pdf else 'None'}")

        # Step 2: Threshold Check
        is_heavy, total_pages = is_heavy_report(annual_pdf)

        board_slice_path = None
        financial_slice_path = None

        if is_heavy:
            logger.warning(f"⚠️  HEAVY REPORT DETECTED ({total_pages} pages > {HEAVY_REPORT_THRESHOLD})")
            logger.info("Engaging Smart Mapping Strategy with gemini-3-pro-preview...")

            # A. Run AI-powered structure mapper (uses gemini-3-pro-preview with retry)
            structure_map = map_report_structure(annual_pdf, model)

            # B. Create targeted slices
            logger.info("Step 2b: Creating targeted PDF slices...")
            slices = create_report_slices(annual_pdf, structure_map, company_dir)

            board_slice_path = slices.get('board_slice')
            financial_slice_path = slices.get('financial_slice')

            if board_slice_path:
                temp_files.append(board_slice_path)
            if financial_slice_path:
                temp_files.append(financial_slice_path)

            if not board_slice_path and not financial_slice_path:
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
            if board_slice_path:
                board_slice_uri = upload_pdf_to_gemini(board_slice_path)
            if financial_slice_path:
                financial_slice_uri = upload_pdf_to_gemini(financial_slice_path)

            if board_slice_uri is None and financial_slice_uri is None:
                logger.error(f"Failed to upload any slices for {company_name}")
                return False, ["UPLOAD_FAILED"]
        else:
            full_annual_uri = upload_pdf_to_gemini(annual_pdf)
            if full_annual_uri is None:
                logger.error(f"Failed to upload annual report for {company_name}")
                return False, ["UPLOAD_FAILED"]

        if quarterly_pdf:
            quarterly_uri = upload_pdf_to_gemini(quarterly_pdf)

        # Step 4: Generate sections
        logger.info("Step 4: Generating sections...")
        html_sections = []

        for section_id in SECTIONS:
            if is_heavy:
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
        final_html = get_html_template(company_name)
        final_html += "\n".join(html_sections)
        final_html += get_html_footer()

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
        for temp_file in temp_files:
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {temp_file}: {e}")


def main():
    """Main entry point for batch report generation."""
    logger.info("=" * 60)
    logger.info("Starting Batch Financial Report Generator")
    logger.info(f"Model: {MODEL_NAME} (single model for all operations)")
    logger.info("=" * 60)

    if not SUPABASE_ANON_KEY:
        logger.error("SUPABASE_ANON_KEY environment variable is not set")
        return

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY environment variable is not set")
        return

    # Configure Gemini and get model instance
    try:
        model = configure_gemini()
    except Exception as e:
        logger.error(f"Failed to configure Gemini: {e}")
        return

    if not FINANCIAL_REPORTS_DIR.exists():
        logger.error(f"Financial Reports directory not found: {FINANCIAL_REPORTS_DIR}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    company_dirs = [d for d in FINANCIAL_REPORTS_DIR.iterdir() if d.is_dir()]

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

    fully_successful = 0
    partial_success = 0
    total_failed = 0
    all_failures = {}

    for company_dir in sorted(company_dirs):
        try:
            success, failed_sections = process_company(company_dir, model)
            if success:
                fully_successful += 1
            elif failed_sections and failed_sections[0] not in ["NO_FILES", "UPLOAD_FAILED"]:
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
    logger.info(f"Reports saved to: {OUTPUT_DIR}")

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
