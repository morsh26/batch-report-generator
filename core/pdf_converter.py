#!/usr/bin/env python3
"""
PDF Converter Module.

Converts HTML reports to PDF format using weasyprint.
Handles RTL Hebrew text and embedded styling.
"""

import logging
from typing import Optional

try:
    from weasyprint import HTML, CSS
    from weasyprint.text.fonts import FontConfiguration
    WEASYPRINT_AVAILABLE = True
except ImportError:
    WEASYPRINT_AVAILABLE = False

logger = logging.getLogger(__name__)


def html_to_pdf(html_content: str) -> Optional[bytes]:
    """
    Convert HTML string to PDF bytes.

    Args:
        html_content: Complete HTML document string

    Returns:
        PDF as bytes, or None if conversion fails
    """
    if not WEASYPRINT_AVAILABLE:
        logger.error("weasyprint is not installed. Run: pip install weasyprint")
        return None

    try:
        font_config = FontConfiguration()

        # Create HTML document from string
        html_doc = HTML(string=html_content)

        # Render to PDF bytes
        pdf_bytes = html_doc.write_pdf(font_config=font_config)

        logger.info(f"PDF generated successfully ({len(pdf_bytes):,} bytes)")
        return pdf_bytes

    except Exception as e:
        logger.error(f"PDF conversion failed: {e}")
        return None
