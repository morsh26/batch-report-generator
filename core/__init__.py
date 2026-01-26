"""
Core module for Financial Reports Service.

This module contains all business logic for processing financial reports.
The functions here are stateless and do not interact with the file system directly.

Modules:
- config: Settings and constants
- ai_engine: Gemini API interaction and retry logic
- pdf_processor: PyMuPDF slicing and text extraction
- prompts: AI prompts for the 8 sections
- report_builder: HTML report assembly
"""

from .config import (
    GOOGLE_API_KEY,
    SUPABASE_FUNCTION_URL,
    SUPABASE_ANON_KEY,
    MODEL_NAME,
    SECTIONS,
    BOARD_REPORT_SECTIONS,
    FINANCIAL_STATEMENTS_SECTIONS,
    SECTION_DISPLAY_NAMES,
    MAX_RETRIES,
    BASE_DELAY,
    MAX_DELAY,
    API_DELAY,
    HEAVY_REPORT_THRESHOLD,
    TOC_SCAN_PAGES,
    DEFAULT_FINANCIAL_REPORTS_DIR,
    DEFAULT_OUTPUT_DIR,
    validate_config,
)

from .ai_engine import (
    configure_gemini,
    get_model,
    generate_with_retry,
    upload_pdf_to_gemini,
    generate_section_with_fallback,
)

from .pdf_processor import (
    get_pdf_page_count,
    is_heavy_report,
    extract_toc_text,
    slice_pdf,
    map_report_structure,
    create_report_slices,
    get_default_structure_map,
)

from .report_builder import (
    get_html_template,
    get_html_footer,
    assemble_report,
    create_error_section,
)

__all__ = [
    # Config
    'GOOGLE_API_KEY',
    'SUPABASE_FUNCTION_URL',
    'SUPABASE_ANON_KEY',
    'MODEL_NAME',
    'SECTIONS',
    'BOARD_REPORT_SECTIONS',
    'FINANCIAL_STATEMENTS_SECTIONS',
    'SECTION_DISPLAY_NAMES',
    'MAX_RETRIES',
    'BASE_DELAY',
    'MAX_DELAY',
    'API_DELAY',
    'HEAVY_REPORT_THRESHOLD',
    'TOC_SCAN_PAGES',
    'DEFAULT_FINANCIAL_REPORTS_DIR',
    'DEFAULT_OUTPUT_DIR',
    'validate_config',
    # AI Engine
    'configure_gemini',
    'get_model',
    'generate_with_retry',
    'upload_pdf_to_gemini',
    'generate_section_with_fallback',
    # PDF Processor
    'get_pdf_page_count',
    'is_heavy_report',
    'extract_toc_text',
    'slice_pdf',
    'map_report_structure',
    'create_report_slices',
    'get_default_structure_map',
    # Report Builder
    'get_html_template',
    'get_html_footer',
    'assemble_report',
    'create_error_section',
]
