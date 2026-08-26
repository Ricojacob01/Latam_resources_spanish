import re
from pyspark.sql import SparkSession
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, HRFlowable, PageBreak
)

spark = SparkSession.builder.getOrCreate()

FIELD_PATTERNS = {
    "ID":      r"ID of the property:\s*(.+?)(?=\n|Name of the property:|$)",
    "Name":    r"Name of the property:\s*(.+?)(?=\n|Summary of the property:|$)",
    "Summary": r"Summary of the property:\s*([\s\S]+)",
}


def _parse_listing(text: str) -> dict:
    result = {}
    for key, pattern in FIELD_PATTERNS.items():
        m = re.search(pattern, text, re.IGNORECASE)
        result[key] = m.group(1).strip() if m else ""
    if not any(result.values()):
        result["Summary"] = text.strip()
    return result


def create_listings_pdf(
    table_name: str,
    column_name: str = "listing_source_information",
    output_path: str = "./airbnb_listings.pdf",
    rows_limit: int = 50,
) -> str:
    """
    Read listing data from a Delta table and write a formatted PDF.

    Parameters
    ----------
    table_name : str
        Fully-qualified or active-schema Delta table name.
    column_name : str
        Column containing the listing text. Default: 'listing_source_information'.
    output_path : str
        Destination path for the PDF file.
    rows_limit : int
        Maximum number of listings to include.

    Returns
    -------
    str
        Path where the PDF was written.
    """
    # Load data
    df = spark.table(table_name).select(column_name)
    if rows_limit:
        df = df.limit(rows_limit)
    rows = [row[column_name] for row in df.collect() if row[column_name]]
    print(f"  Loaded {len(rows)} rows from {table_name}.{column_name}")

    # Document setup
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.85 * inch,
        rightMargin=0.85 * inch,
        topMargin=0.9 * inch,
        bottomMargin=0.9 * inch,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "DocTitle", parent=styles["Title"],
        fontSize=20, textColor=colors.HexColor("#1A1A2E"), spaceAfter=6,
    )
    subtitle_style = ParagraphStyle(
        "DocSubtitle", parent=styles["Normal"],
        fontSize=11, textColor=colors.HexColor("#555555"), spaceAfter=20,
    )
    card_id_style = ParagraphStyle(
        "CardID", parent=styles["Normal"],
        fontSize=9, textColor=colors.HexColor("#888888"), spaceBefore=4,
    )
    card_name_style = ParagraphStyle(
        "CardName", parent=styles["Heading2"],
        fontSize=14, textColor=colors.HexColor("#1A1A2E"), spaceBefore=2, spaceAfter=6,
    )
    label_style = ParagraphStyle(
        "Label", parent=styles["Normal"],
        fontSize=9, textColor=colors.HexColor("#E07B39"),
        fontName="Helvetica-Bold", spaceBefore=4, spaceAfter=2,
    )
    body_style = ParagraphStyle(
        "Body", parent=styles["Normal"],
        fontSize=10, textColor=colors.HexColor("#333333"), leading=15, spaceAfter=8,
    )

    story = []

    # Document header
    story.append(Paragraph("Airbnb Listings", title_style))
    story.append(Paragraph(
        f"Source: <b>{table_name}</b> &nbsp;|&nbsp; Column: <b>{column_name}</b> &nbsp;|&nbsp; {len(rows)} records",
        subtitle_style,
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor("#E07B39")))
    story.append(Spacer(1, 18))

    # One card per listing
    for i, raw_text in enumerate(rows):
        fields = _parse_listing(raw_text)

        if fields.get("ID"):
            story.append(Paragraph(f"Property ID: {fields['ID']}", card_id_style))
        if fields.get("Name"):
            story.append(Paragraph(fields["Name"], card_name_style))
        if fields.get("Summary"):
            story.append(Paragraph("SUMMARY", label_style))
            safe_summary = (
                fields["Summary"]
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            story.append(Paragraph(safe_summary, body_style))

        if i < len(rows) - 1:
            story.append(PageBreak())

    doc.build(story)
    print(f"  PDF saved to: {output_path}")
    return output_path
