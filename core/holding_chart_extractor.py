#!/usr/bin/env python3
"""
Holding Chart Extractor Module.

Extracts the Company Ownership Structure Chart (Holding Tree) from financial
report PDFs using Vision LLM analysis. The strategy is to identify the page
containing the chart and render it as a full-page high-quality image.

This avoids the complexity of cropping vector graphics which often results
in cut-off text and low accuracy.
"""

import base64
import io
import json
import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Check for optional dependencies
try:
    from pdf2image import convert_from_bytes
    from pdf2image.exceptions import PDFPageCountError, PDFSyntaxError
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False
    logger.warning("pdf2image not installed. Holding chart extraction disabled.")

try:
    import google.generativeai as genai
    from PIL import Image
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logger.warning("google-generativeai not installed. Holding chart extraction disabled.")


# =============================================================================
# PYDANTIC MODELS FOR STRUCTURED OUTPUT
# =============================================================================

class HoldingChartResult(BaseModel):
    """Structured response from the Vision LLM for chart detection."""
    found: bool = Field(description="Whether a holding/ownership chart was found")
    page_number: int = Field(default=0, description="1-based page number where chart was found")
    confidence: str = Field(default="none", description="Confidence level: high, medium, low, none")
    reasoning: str = Field(default="", description="Brief explanation of the finding")


# =============================================================================
# CONSTANTS
# =============================================================================

# Scanning parameters
SCAN_PAGES_LIMIT = 50  # Only scan first N pages for the chart
LOW_RES_DPI = 75       # DPI for fast scanning
HIGH_RES_DPI = 300     # DPI for final extraction

# Gemini model for vision analysis
VISION_MODEL = "gemini-2.0-flash"

# Prompt for the vision analysis
VISION_PROMPT = """You are a financial analyst specializing in analyzing Hebrew financial reports.
Your task is to identify pages containing Company Ownership Structure Charts (Holding Trees).

These charts typically show:
- The corporate structure with parent and subsidiary companies
- Boxes/rectangles containing company names connected by lines
- Percentage ownership figures between entities
- A hierarchical tree-like visual structure

Common Hebrew titles for these charts include:
- תרשים אחזקות (Ownership Chart)
- מבנה הקבוצה (Group Structure)
- מבנה ההחזקות (Holdings Structure)
- שיעור החזקות (Ownership Percentages)
- מבנה החברה (Company Structure)

Analyze these PDF pages and find the page containing the Company Ownership Structure Chart (Holding Tree diagram).

Look for:
1. A visual diagram with boxes/rectangles containing company names
2. Lines connecting the boxes showing ownership relationships
3. Percentage numbers indicating ownership stakes
4. A hierarchical tree structure (usually top-down or organizational)

IMPORTANT: Look for VISUAL DIAGRAMS, not tables with ownership data.

Return your answer in this exact JSON format:
{
    "found": true or false,
    "page_number": <1-based page number, or 0 if not found>,
    "confidence": "high", "medium", "low", or "none",
    "reasoning": "<brief explanation>"
}

Only return the JSON, nothing else."""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _parse_llm_response(response_text: str) -> HoldingChartResult:
    """Parse LLM response into structured result."""
    try:
        # Try to extract JSON from the response
        text = response_text.strip()

        # Handle markdown code blocks
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        data = json.loads(text)
        return HoldingChartResult(**data)
    except Exception as e:
        logger.warning(f"Failed to parse LLM response: {e}")
        return HoldingChartResult(
            found=False,
            page_number=0,
            confidence="none",
            reasoning=f"Failed to parse response: {str(e)}"
        )


# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

def extract_holding_chart_page(
    pdf_bytes: bytes,
    output_dir: Path,
    google_api_key: str,
    company_name: str = "company"
) -> Optional[str]:
    """
    Extract the Company Ownership Structure Chart from a PDF.

    Strategy:
    1. Fast scan first 50 pages at low resolution
    2. Use Gemini Vision to identify the chart page
    3. Extract that page at high resolution
    4. Save as PNG for inclusion in reports

    Args:
        pdf_bytes: The PDF file content as bytes
        output_dir: Directory to save the extracted chart image
        google_api_key: Google API key for Gemini Vision
        company_name: Company name for the output filename

    Returns:
        Path to the saved chart image, or None if not found
    """
    # Check dependencies
    if not PDF2IMAGE_AVAILABLE:
        logger.error("pdf2image is required for holding chart extraction")
        return None

    if not GEMINI_AVAILABLE:
        logger.error("google-generativeai is required for holding chart extraction")
        return None

    if not google_api_key:
        logger.error("Google API key is required for holding chart extraction")
        return None

    try:
        # =====================================================================
        # STEP 1: Fast Scan - Convert first N pages to low-res images
        # =====================================================================
        logger.info(f"Holding Chart: Scanning first {SCAN_PAGES_LIMIT} pages at {LOW_RES_DPI} DPI...")

        try:
            # Convert PDF pages to images (in memory)
            images = convert_from_bytes(
                pdf_bytes,
                dpi=LOW_RES_DPI,
                fmt="jpeg",
                first_page=1,
                last_page=SCAN_PAGES_LIMIT,
                thread_count=4
            )
        except PDFPageCountError:
            logger.warning("PDF has fewer pages than scan limit, scanning all pages")
            images = convert_from_bytes(
                pdf_bytes,
                dpi=LOW_RES_DPI,
                fmt="jpeg",
                thread_count=4
            )
        except PDFSyntaxError as e:
            logger.error(f"PDF is corrupted or encrypted: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to convert PDF to images: {e}")
            return None

        if not images:
            logger.error("No pages could be extracted from PDF")
            return None

        logger.info(f"Converted {len(images)} pages to images")

        # =====================================================================
        # STEP 2: AI Analysis - Send images to Gemini Vision
        # =====================================================================
        logger.info("Holding Chart: Analyzing pages with Gemini Vision...")

        # Configure Gemini
        genai.configure(api_key=google_api_key)
        model = genai.GenerativeModel(VISION_MODEL)

        # Prepare content with images and page labels
        content_parts = []
        for i, img in enumerate(images):
            content_parts.append(f"[Page {i + 1}]")
            content_parts.append(img)  # PIL Image directly supported by Gemini

        content_parts.append(VISION_PROMPT)

        try:
            response = model.generate_content(content_parts)
            response_text = response.text
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return None

        # =====================================================================
        # STEP 3: Parse Structured Output
        # =====================================================================
        logger.info("Holding Chart: Parsing AI response...")

        result = _parse_llm_response(response_text)

        logger.info(f"AI Result: found={result.found}, page={result.page_number}, "
                   f"confidence={result.confidence}, reason={result.reasoning}")

        # =====================================================================
        # STEP 4: High-Res Extraction (if found)
        # =====================================================================
        if not result.found or result.confidence == "none":
            logger.info("No holding chart found in the PDF")
            return None

        if result.confidence == "low":
            logger.warning("Low confidence detection - extracting anyway")

        page_num = result.page_number
        if page_num < 1 or page_num > len(images):
            logger.error(f"Invalid page number: {page_num}")
            return None

        logger.info(f"Holding Chart: Extracting page {page_num} at {HIGH_RES_DPI} DPI...")

        try:
            high_res_images = convert_from_bytes(
                pdf_bytes,
                dpi=HIGH_RES_DPI,
                fmt="png",
                first_page=page_num,
                last_page=page_num,
                thread_count=2
            )
        except Exception as e:
            logger.error(f"Failed to extract high-res page: {e}")
            return None

        if not high_res_images:
            logger.error("Failed to render high-res page")
            return None

        # =====================================================================
        # STEP 5: Save the image
        # =====================================================================
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Sanitize company name for filename
        safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in company_name)
        safe_name = safe_name.strip().replace(" ", "_")[:50]

        output_path = output_dir / f"{safe_name}_holding_chart.png"

        high_res_images[0].save(output_path, format="PNG", optimize=True)

        logger.info(f"Holding chart saved to: {output_path}")

        return str(output_path)

    except Exception as e:
        logger.error(f"Unexpected error in holding chart extraction: {e}")
        return None


def create_holding_chart_html(image_path: Optional[str], company_name: str) -> str:
    """
    Create HTML section for the holding chart.

    Args:
        image_path: Path to the chart image, or None if not found
        company_name: Company name for the section title

    Returns:
        HTML string for the holding chart section
    """
    if image_path is None:
        return """
<div class="section">
    <h2>מבנה אחזקות</h2>
    <p class="info">לא נמצא תרשים מבנה אחזקות בדוח.</p>
</div>
"""

    # Read image and convert to base64 for embedding in HTML
    try:
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        return f"""
<div class="section holding-chart-section">
    <h2>מבנה אחזקות</h2>
    <div class="holding-chart-container">
        <img src="data:image/png;base64,{image_data}"
             alt="תרשים מבנה אחזקות - {company_name}"
             class="holding-chart-image" />
    </div>
</div>
"""
    except Exception as e:
        logger.error(f"Failed to embed holding chart image: {e}")
        return """
<div class="section">
    <h2>מבנה אחזקות</h2>
    <p class="error">שגיאה בטעינת תרשים מבנה האחזקות.</p>
</div>
"""
