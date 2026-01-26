"""
Prompts module for Financial Reports Service.

Contains all AI prompts used for report analysis and generation.
"""


def get_structure_mapping_prompt(total_pages: int, toc_text: str) -> str:
    """
    Generate the prompt for AI-powered structure mapping of financial reports.

    Args:
        total_pages: Total number of pages in the PDF
        toc_text: Extracted text from the TOC pages

    Returns:
        The formatted prompt string
    """
    return f"""You are analyzing the Table of Contents of an Israeli financial report.
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
