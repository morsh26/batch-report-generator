"""
Configuration module for Financial Reports Service.

Loads environment variables and defines all constants used across the application.
Both CLI and Server can import settings from here.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# API CONFIGURATION
# =============================================================================

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
SUPABASE_FUNCTION_URL = os.getenv(
    "SUPABASE_FUNCTION_URL",
    "https://your-project.supabase.co/functions/v1/generate-compliance-report-v2"
)
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

# =============================================================================
# MODEL CONFIGURATION
# =============================================================================

MODEL_NAME = "gemini-3-pro-preview"

# =============================================================================
# REPORT SECTIONS
# =============================================================================

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

# Section routing for heavy reports - which sections use which PDF slice
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

# Section display names (Hebrew)
SECTION_DISPLAY_NAMES = {
    'company_profile': 'פרופיל חברה',
    'executive_summary': 'תקציר מנהלים',
    'business_environment': 'סביבה עסקית',
    'asset_portfolio_analysis': 'ניתוח תיק נכסים',
    'debt_structure': 'מבנה חוב',
    'financial_analysis': 'ניתוח פיננסי',
    'cash_flow_and_liquidity': 'תזרים מזומנים ונזילות',
    'liquidation_analysis': 'ניתוח פירוק'
}

# =============================================================================
# RETRY CONFIGURATION - EXPONENTIAL BACKOFF
# =============================================================================

MAX_RETRIES = 6
BASE_DELAY = 30  # Starting delay in seconds
MAX_DELAY = 600  # Maximum delay (10 minutes)
API_DELAY = 5.0  # Delay between API calls (seconds)

# =============================================================================
# PDF PROCESSING CONFIGURATION
# =============================================================================

HEAVY_REPORT_THRESHOLD = 300  # Pages threshold for "heavy" reports
TOC_SCAN_PAGES = 30  # Number of pages to scan for TOC

# =============================================================================
# DEFAULT PATHS (can be overridden via environment variables)
# =============================================================================

DEFAULT_FINANCIAL_REPORTS_DIR = Path(os.getenv("FINANCIAL_REPORTS_DIR", "./Financial_Reports"))
DEFAULT_OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./All_Reports"))


def validate_config() -> tuple[bool, list[str]]:
    """
    Validate that required configuration is present.

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    if not GOOGLE_API_KEY:
        errors.append("GOOGLE_API_KEY environment variable is not set")

    if not SUPABASE_ANON_KEY:
        errors.append("SUPABASE_ANON_KEY environment variable is not set")

    return len(errors) == 0, errors
