"""
AI Engine module for Financial Reports Service.

Handles all Gemini API interactions with robust exponential backoff retry logic.
This module is stateless - it does not interact with the file system directly.
"""

import time
import logging
from typing import Optional

import requests
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

from .config import (
    GOOGLE_API_KEY,
    MODEL_NAME,
    SUPABASE_FUNCTION_URL,
    SUPABASE_ANON_KEY,
    MAX_RETRIES,
    BASE_DELAY,
    MAX_DELAY,
    SECTION_DISPLAY_NAMES,
)

logger = logging.getLogger(__name__)

# Global model instance - initialized once
_gemini_model: Optional[genai.GenerativeModel] = None


# =============================================================================
# INITIALIZATION
# =============================================================================

def configure_gemini() -> genai.GenerativeModel:
    """
    Configure Google Generative AI and return the model instance.

    Returns:
        Configured GenerativeModel instance

    Raises:
        ValueError: If GOOGLE_API_KEY is not set
    """
    global _gemini_model

    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY environment variable is not set")

    genai.configure(api_key=GOOGLE_API_KEY)
    _gemini_model = genai.GenerativeModel(MODEL_NAME)

    logger.info(f"Gemini API configured successfully with model: {MODEL_NAME}")
    return _gemini_model


def get_model() -> Optional[genai.GenerativeModel]:
    """Get the current Gemini model instance."""
    return _gemini_model


# =============================================================================
# CORE RETRY WRAPPER
# =============================================================================

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
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(
                f"⏳ Rate limit hit for {operation_name}. "
                f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
            )
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except google_exceptions.ServiceUnavailable as e:
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(
                f"⏳ Service unavailable for {operation_name}. "
                f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
            )
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except google_exceptions.DeadlineExceeded as e:
            wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
            logger.warning(
                f"⏳ Timeout for {operation_name}. "
                f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
            )
            time.sleep(wait_time)
            last_error = str(e)
            continue

        except Exception as e:
            error_str = str(e).lower()

            # Check if it's a rate limit error in the message
            if '429' in error_str or 'resource' in error_str or 'exhausted' in error_str or 'quota' in error_str:
                wait_time = min(base_delay * (2 ** attempt), MAX_DELAY)
                logger.warning(
                    f"⏳ Rate limit (from error message) for {operation_name}. "
                    f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
                )
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


# =============================================================================
# FILE UPLOAD TO GEMINI
# =============================================================================

def upload_pdf_to_gemini(
    pdf_bytes: bytes,
    display_name: str,
    max_retries: int = 5
) -> Optional[str]:
    """
    Upload PDF bytes to Gemini with retry logic.

    Args:
        pdf_bytes: The PDF file content as bytes
        display_name: Display name for the uploaded file
        max_retries: Maximum number of upload attempts

    Returns:
        The file URI if successful, None otherwise
    """
    import tempfile
    import os

    logger.info(f"Uploading {display_name} to Gemini...")

    # Create a temporary file to upload (Gemini SDK requires a file path)
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as f:
            f.write(pdf_bytes)
            temp_file = f.name

        for attempt in range(max_retries):
            try:
                uploaded_file = genai.upload_file(
                    path=temp_file,
                    display_name=display_name
                )

                logger.info(f"Waiting for {display_name} to be processed...")
                while uploaded_file.state.name == "PROCESSING":
                    time.sleep(2)
                    uploaded_file = genai.get_file(uploaded_file.name)

                if uploaded_file.state.name == "ACTIVE":
                    logger.info(f"Successfully uploaded {display_name}: {uploaded_file.uri}")
                    return uploaded_file.uri
                else:
                    logger.error(
                        f"File {display_name} failed to process. State: {uploaded_file.state.name}"
                    )
                    return None

            except Exception as e:
                error_str = str(e).lower()
                if '429' in error_str or 'resource' in error_str or 'exhausted' in error_str:
                    wait_time = BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        f"⏳ Rate limit on upload. "
                        f"Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
                    )
                    time.sleep(wait_time)
                    continue

                logger.warning(f"Upload attempt {attempt + 1} failed for {display_name}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error(f"Failed to upload {display_name} after {max_retries} attempts")
                    return None

        return None

    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except Exception:
                pass


# =============================================================================
# SECTION GENERATION VIA SUPABASE EDGE FUNCTION
# =============================================================================

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
    rate_limit_keywords = [
        'too many requests', 'rate limit', 'quota', '429', 'resource exhausted'
    ]
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

    Args:
        section_id: The section identifier
        file_uri1: Primary PDF file URI
        file_uri2: Secondary PDF file URI (optional)
        company_name: Name of the company
        display_name: Display name for logging

    Returns:
        Tuple of (html_content, is_token_error)
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
                    logger.warning(
                        f"⏳ Rate limit hit for {display_name}. "
                        f"Waiting {wait_time}s before retry {rate_limit_retries}/{MAX_RETRIES}..."
                    )
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
                    logger.warning(
                        f"Attempt {general_retries} failed for {display_name} (500 error), "
                        f"retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"API error for {display_name}: "
                        f"{response.status_code} - {response.text[:200]}"
                    )
                    return None, False

            logger.error(
                f"API error for {display_name}: {response.status_code} - {response.text[:200]}"
            )
            return None, False

        except requests.exceptions.Timeout:
            general_retries += 1
            if general_retries < max_general_retries:
                logger.warning(
                    f"Timeout on attempt {general_retries} for {display_name}, retrying..."
                )
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

    Args:
        section_id: The section identifier
        primary_uri: Primary PDF file URI
        secondary_uri: Secondary PDF file URI (optional)
        fallback_uri: Fallback PDF URI if token limit is hit
        company_name: Name of the company

    Returns:
        HTML string for the section (or error div)
    """
    display_name = SECTION_DISPLAY_NAMES.get(section_id, section_id)
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
            logger.error(
                f"    ❌ {display_name} failed - token limit exceeded even with single file"
            )
            return (
                f'<div class="error">שגיאה: {display_name} - '
                f'הקובץ גדול מדי גם עם קובץ בודד</div>'
            )
        else:
            logger.error(f"    ❌ {display_name} failed on fallback")
            return f'<div class="error">שגיאה בייצור {display_name} (fallback נכשל)</div>'

    logger.error(f"    ❌ {display_name} failed (not a token limit error)")
    return f'<div class="error">שגיאה בייצור {display_name}</div>'
