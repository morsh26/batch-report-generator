"""
Report Builder module for Financial Reports Service.
Professional Design Edition - WeasyPrint Optimized.
Uses @page:first for cover, standard margins for content.
"""

import time
from typing import List

HEBREW_MONTHS = [
    'ינואר', 'פברואר', 'מרץ', 'אפריל', 'מאי', 'יוני',
    'יולי', 'אוגוסט', 'ספטמבר', 'אוקטובר', 'נובמבר', 'דצמבר'
]

def get_html_template(company_name: str, timestamp: str = None) -> str:
    """
    Generate the HTML header optimized for WeasyPrint PDF rendering.
    """
    # Replace underscores with spaces for display
    display_name = company_name.replace('_', ' ')

    if timestamp is None:
        month_idx = int(time.strftime('%m')) - 1
        year = time.strftime('%Y')
        timestamp = f"{HEBREW_MONTHS[month_idx]} {year}"

    return f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
    <meta charset="UTF-8">
    <title>דוח אנליזה - {company_name}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Assistant:wght@300;400;600&family=Heebo:wght@400;700&display=swap');

        :root {{
            --primary: #0f2b46;
            --accent: #c5a47e;
            --text: #2d3748;
            --bg-gray: #f7fafc;
            --border: #e2e8f0;
        }}

        /* --- Page Rules for WeasyPrint --- */

        /* First page (cover): zero margins for full bleed */
        @page:first {{
            size: A4;
            margin: 0;
        }}

        /* All other pages: standard margins */
        @page {{
            size: A4;
            margin: 25mm 20mm 25mm 20mm;
        }}

        * {{
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Assistant', sans-serif;
            color: var(--text);
            line-height: 1.6;
            margin: 0;
            padding: 0;
            background-color: white;
        }}

        /* --- Typography --- */
        h1 {{
            font-family: 'Heebo', sans-serif;
            color: var(--primary);
        }}

        h2 {{
            font-family: 'Heebo', sans-serif;
            color: var(--primary);
            font-size: 22px;
            border-bottom: 2px solid var(--accent);
            padding-bottom: 10px;
            margin-bottom: 20px;
            break-before: page;
            page-break-before: always;
            margin-top: 0;
            padding-top: 10px;
        }}

        h3 {{
            font-family: 'Heebo', sans-serif;
            color: var(--primary);
            font-size: 18px;
            margin-top: 30px;
            page-break-after: avoid;
        }}

        p {{
            margin-bottom: 15px;
            text-align: justify;
        }}

        ul, ol {{
            margin-bottom: 15px;
            padding-right: 20px;
        }}

        /* --- Cover Page (Full Bleed) --- */
        .cover-page {{
            /* Fill the entire first page */
            width: 210mm;
            height: 297mm;
            margin: 0;
            padding: 0;

            /* Background */
            background: linear-gradient(135deg, var(--primary) 0%, #1a365d 100%);
            color: white;

            /* Force page break after */
            page-break-after: always;
            break-after: page;
        }}

        .cover-inner {{
            /* Inner container for centering content */
            width: 100%;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            padding: 40mm;
        }}

        .cover-title h1 {{
            font-family: 'Heebo', sans-serif;
            font-size: 48px;
            margin: 0 0 25px 0;
            line-height: 1.2;
            color: white;
            border: none;
            padding: 0;
        }}

        .cover-subtitle {{
            font-family: 'Heebo', sans-serif;
            font-size: 26px;
            color: var(--accent);
            font-weight: 300;
            letter-spacing: 1px;
        }}

        /* --- Tables --- */
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 13px;
            page-break-inside: auto;
        }}

        tr {{
            page-break-inside: avoid;
            page-break-after: auto;
        }}

        th {{
            background-color: var(--primary);
            color: white;
            font-weight: 600;
            text-align: right;
            padding: 10px;
        }}

        td {{
            padding: 8px 10px;
            border-bottom: 1px solid var(--border);
        }}

        tr:nth-child(even) {{
            background-color: var(--bg-gray);
        }}

        /* --- Error Section --- */
        .error {{
            border: 1px solid #e53e3e;
            background: #fff5f5;
            color: #c53030;
            padding: 15px;
            margin: 20px 0;
            page-break-inside: avoid;
        }}

        /* --- Holding Chart Section --- */
        .holding-chart-section {{
            page-break-inside: avoid;
            break-inside: avoid;
        }}

        .holding-chart-section h2 {{
            /* Override: don't break before h2 in this section */
            break-before: auto;
            page-break-before: auto;
            margin-bottom: 15px;
        }}

        .holding-chart-container {{
            page-break-inside: avoid;
            break-inside: avoid;
            margin: 10px 0;
            padding: 10px;
            background: white;
            border: 1px solid var(--border);
            border-radius: 4px;
            text-align: center;
        }}

        .holding-chart-image {{
            max-width: 100%;
            /* Fit image within page height minus header and margins */
            max-height: 200mm;
            width: auto;
            height: auto;
            display: block;
            margin: 0 auto;
        }}

        /* --- Print Adjustments --- */
        @media print {{
            body {{ background: white; }}
            a {{ text-decoration: none; color: inherit; }}
        }}
    </style>
</head>
<body>

<div class="cover-page">
    <div class="cover-inner">
        <div class="cover-title">
            <h1>דוח אנליזה - {display_name}</h1>
            <div class="cover-subtitle">{timestamp}</div>
        </div>
    </div>
</div>

<div class="content">
"""

def get_html_footer() -> str:
    """
    Generate the HTML footer.
    """
    return """
</div>

<div style="text-align: center; color: #718096; font-size: 10px; margin-top: 50px; border-top: 1px solid #e2e8f0; padding-top: 10px;">
    דוח זה הופק אוטומטית ע"י מערכת AI. המידע המוצג הינו למטרות ניתוח בלבד.
</div>
</body>
</html>
"""

def assemble_report(company_name: str, sections_html: List[str], timestamp: str = None) -> str:
    """
    Assemble a complete HTML report from sections.
    """
    html = get_html_template(company_name, timestamp)
    html += "\n".join(sections_html)
    html += get_html_footer()
    return html

def create_error_section(section_id: str, display_name: str, error_type: str = "general") -> str:
    """
    Create a styled error placeholder.
    """
    error_messages = {
        "token_limit": f"שגיאת עומס: {display_name} - הקובץ גדול מדי.",
        "fallback_failed": f"נכשל ניסיון יצירת {display_name}.",
        "general": f"לא ניתן היה לייצר את {display_name}."
    }
    return f'''
    <div class="error">
        <strong>⚠ {display_name}</strong>: {error_messages.get(error_type, error_messages["general"])}
    </div>
    '''
