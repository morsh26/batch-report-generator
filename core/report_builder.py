"""
Report Builder module for Financial Reports Service.

Handles HTML report assembly and template generation.
This module is stateless - returns strings without file I/O.
"""

import time
from typing import List

HEBREW_MONTHS = [
    'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני',
    'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר'
]


def get_html_template(company_name: str, timestamp: str = None) -> str:
    """
    Generate the HTML header template with RTL support and Hebrew fonts.

    Args:
        company_name: Name of the company for the report
        timestamp: Optional timestamp string. If None, uses current time.

    Returns:
        HTML header string
    """
    if timestamp is None:
        month_idx = int(time.strftime('%m')) - 1
        year = time.strftime('%Y')
        timestamp = f"{HEBREW_MONTHS[month_idx]} {year}"

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
            margin: 0;
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
            padding: 20px 0;
            background: white;
        }}

        /* Cover page styles */
        .cover-page {{
            text-align: center;
            padding-top: 40vh;
            padding-bottom: 40vh;
            page-break-after: always;
        }}

        .cover-page h1 {{
            font-size: 28px;
            border-bottom: none;
            margin-bottom: 20px;
        }}

        .cover-page .meta-info {{
            font-size: 16px;
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

        .info {{
            color: #718096;
            font-style: italic;
        }}

        /* Holding Chart styles */
        .holding-chart-section {{
            text-align: center;
        }}

        .holding-chart-container {{
            margin: 20px 0;
            padding: 10px;
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
        }}

        .holding-chart-image {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 0 auto;
        }}

        /* Print/PDF styles */
        @page {{
            size: A4;
            margin: 15mm;
        }}

        @media print {{
            body {{
                background-color: white;
                margin: 0;
                padding: 0;
            }}

            .report-container {{
                box-shadow: none;
                padding: 0;
                border-radius: 0;
            }}

            table {{
                page-break-inside: auto;
            }}

            tr {{
                page-break-inside: avoid;
                page-break-after: auto;
            }}

            thead {{
                display: table-header-group;
            }}

            .section {{
                page-break-inside: avoid;
            }}

            h2, h3 {{
                page-break-after: avoid;
            }}

            .cover-page {{
                padding-top: 45%;
                padding-bottom: 45%;
                page-break-after: always;
            }}
        }}
    </style>
</head>
<body>
    <div class="report-container">
        <div class="cover-page">
            <h1>דוח אנליזה - {company_name}</h1>
            <div class="meta-info">{timestamp}</div>
        </div>
"""


def get_html_footer() -> str:
    """
    Generate the HTML footer.

    Returns:
        HTML footer string
    """
    return """
    </div>
</body>
</html>
"""


def assemble_report(company_name: str, sections_html: List[str], timestamp: str = None) -> str:
    """
    Assemble a complete HTML report from sections.

    Args:
        company_name: Name of the company
        sections_html: List of HTML strings for each section
        timestamp: Optional timestamp string

    Returns:
        Complete HTML document as a string
    """
    html = get_html_template(company_name, timestamp)
    html += "\n".join(sections_html)
    html += get_html_footer()
    return html


def create_error_section(section_id: str, display_name: str, error_type: str = "general") -> str:
    """
    Create an error placeholder for a failed section.

    Args:
        section_id: The section identifier
        display_name: Hebrew display name for the section
        error_type: Type of error ("token_limit", "fallback_failed", "general")

    Returns:
        HTML string for the error section
    """
    error_messages = {
        "token_limit": f"שגיאה: {display_name} - הקובץ גדול מדי גם עם קובץ בודד",
        "fallback_failed": f"שגיאה בייצור {display_name} (fallback נכשל)",
        "general": f"שגיאה בייצור {display_name}"
    }

    message = error_messages.get(error_type, error_messages["general"])
    return f'<div class="error">{message}</div>'
