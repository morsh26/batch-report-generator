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
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import requests
import google.generativeai as genai

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

# Delay between API calls (seconds)
API_DELAY = 2.0

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


def call_section_api(
    section_id: str,
    file_uri1: str,
    file_uri2: Optional[str],
    company_name: str,
    display_name: str,
    max_retries: int = 2
) -> tuple[Optional[str], bool]:
    """
    Make API call to generate a section.
    Returns (html_content, is_token_error).
    html_content is None if failed, string if successful.
    is_token_error indicates if failure was due to token limits.
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

    for attempt in range(max_retries):
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

            # Check for token limit error
            if is_token_limit_error(response):
                return None, True

            # Other 500 errors - retry
            if response.status_code == 500 and attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed for {display_name} (500 error), retrying in 5s...")
                time.sleep(5)
                continue

            # Other errors
            logger.error(f"API error for {display_name}: {response.status_code} - {response.text[:200]}")
            return None, False

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                logger.warning(f"Timeout on attempt {attempt + 1} for {display_name}, retrying...")
                time.sleep(5)
                continue
            return None, False
        except Exception as e:
            logger.error(f"Exception generating {display_name}: {e}")
            return None, False

    return None, False


def generate_section(
    section_id: str,
    file_uri1: str,
    file_uri2: Optional[str],
    company_name: str
) -> str:
    """
    Call Supabase Edge Function to generate a report section.
    Implements smart fallback logic for token limit errors:

    Phase 1: Try with BOTH files (annual + quarterly)
    Phase 2: If token limit error, retry with SINGLE file:
        - Descriptive sections (company_profile, business_environment, asset_portfolio_analysis)
          → Use Annual Report only
        - Financial sections (all others)
          → Use Quarterly Report only (or Annual if no quarterly)
    Phase 3: If still fails, return error HTML (don't crash)

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

    # Sections that need historical context (use Annual for fallback)
    descriptive_sections = {'company_profile', 'business_environment', 'asset_portfolio_analysis'}

    # Sections that need fresh numbers (use Quarterly for fallback)
    financial_sections = {'executive_summary', 'financial_analysis', 'liquidation_analysis',
                          'cash_flow_and_liquidity', 'debt_structure'}

    display_name = section_display_names.get(section_id, section_id)
    logger.info(f"Generating section: {display_name} ({section_id})...")

    # ============================================================
    # PHASE 1: Try with BOTH files
    # ============================================================
    logger.info(f"  Phase 1: Attempting with both files...")
    html_content, is_token_error = call_section_api(
        section_id, file_uri1, file_uri2, company_name, display_name
    )

    if html_content:
        logger.info(f"Successfully generated {display_name}")
        return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

    # ============================================================
    # PHASE 2: Fallback to single file if token limit error
    # ============================================================
    if is_token_error:
        logger.warning(f"⚠️  Token Limit Hit for {display_name} - Retrying with single file...")

        if section_id in descriptive_sections:
            # Use Annual Report only (deeper historical context)
            logger.info(f"  Phase 2: Using ANNUAL report only for {display_name}...")
            fallback_uri1 = file_uri1  # Annual
            fallback_uri2 = None
        elif section_id in financial_sections:
            # Use Quarterly Report only (fresher numbers)
            # If no quarterly, fall back to annual
            if file_uri2:
                logger.info(f"  Phase 2: Using QUARTERLY report only for {display_name}...")
                fallback_uri1 = file_uri2  # Quarterly
                fallback_uri2 = None
            else:
                logger.info(f"  Phase 2: No quarterly available, using ANNUAL report for {display_name}...")
                fallback_uri1 = file_uri1
                fallback_uri2 = None
        else:
            # Unknown section - default to annual
            logger.info(f"  Phase 2: Using ANNUAL report only for {display_name}...")
            fallback_uri1 = file_uri1
            fallback_uri2 = None

        html_content, is_token_error_2 = call_section_api(
            section_id, fallback_uri1, fallback_uri2, company_name, display_name
        )

        if html_content:
            logger.info(f"Successfully generated {display_name} (single file fallback)")
            return f'<div class="section" id="{section_id}">\n{html_content}\n</div>'

        # ============================================================
        # PHASE 3: Final failure
        # ============================================================
        if is_token_error_2:
            logger.error(f"❌ {display_name} failed even with single file (token limit still exceeded)")
            return f'<div class="error">שגיאה: {display_name} - הקובץ גדול מדי גם עם קובץ בודד</div>'
        else:
            logger.error(f"❌ {display_name} failed on single file fallback")
            return f'<div class="error">שגיאה בייצור {display_name} (fallback נכשל)</div>'

    # Non-token-limit error in phase 1
    logger.error(f"❌ {display_name} failed (not a token limit error)")
    return f'<div class="error">שגיאה בייצור {display_name}</div>'


def process_company(company_dir: Path) -> tuple[bool, list[str]]:
    """
    Process a single company: upload files, generate sections, save report.
    Returns (success, failed_sections) - success is True only if ALL sections generated successfully.
    """
    company_name = company_dir.name
    failed_sections = []

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing company: {company_name}")
    logger.info(f"{'='*60}")

    # Find PDF files
    annual_pdf, quarterly_pdf = find_pdf_files(company_dir)

    if annual_pdf is None:
        logger.error(f"No PDF files found for {company_name}")
        return False, ["NO_FILES"]

    logger.info(f"Found PDFs - Annual: {annual_pdf.name if annual_pdf else 'None'}, "
                f"Quarterly: {quarterly_pdf.name if quarterly_pdf else 'None'}")

    # Upload PDFs to Gemini
    file_uri1 = upload_pdf_to_gemini(annual_pdf)
    if file_uri1 is None:
        logger.error(f"Failed to upload annual report for {company_name}")
        return False, ["UPLOAD_FAILED"]

    file_uri2 = None
    if quarterly_pdf:
        file_uri2 = upload_pdf_to_gemini(quarterly_pdf)
        if file_uri2 is None:
            logger.warning(f"Failed to upload quarterly report for {company_name}, continuing with annual only")

    # Generate all sections
    html_sections = []
    for section_id in SECTIONS:
        section_html = generate_section(section_id, file_uri1, file_uri2, company_name)
        html_sections.append(section_html)

        # Track failed sections (check if error div was returned)
        if 'class="error"' in section_html:
            failed_sections.append(section_id)

        time.sleep(API_DELAY)  # Rate limiting delay

    # Assemble final HTML
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
