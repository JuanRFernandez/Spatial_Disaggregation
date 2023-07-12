"""Dynamic report creation using reportlab."""
import copy
from datetime import date
import os

from typing import Optional
import pandas as pd
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    PageBreak,
    Table,
    tableofcontents,
)

cwd = os.getcwd()


class Report:
    """Report class."""

    def __init__(
        self, report_title: str, file_path: Optional[str] = "report.pdf"
    ) -> None:
        """Initialize a report."""
        self.filepath = file_path

        self.open()

        self.write_preamble(report_title)

        # self.insert_table_of_contents()

        self.close()

    def open(self) -> None:
        """Open a report"""
        self.doc = SimpleDocTemplate(
            self.filepath,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=18,
        )
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name="Justify", alignment=TA_JUSTIFY))

        self.styles.add(ParagraphStyle(name="Normal_center", alignment=TA_CENTER))

    def write_preamble(self, report_title: str) -> None:
        """Create a font page of the report.

        Configuration taken mostly from following tutorial: https://www.blog.pythonlibrary.org/2010/03/08/a-simple-step-by-step-reportlab-tutorial/
        """
        story = []

        # TODO: format the front page
        # Add LOCALISED logo
        logo = os.path.join(
            cwd, "..", "..", "reports", "figures", "logos", "LOGO_LOCALISED_RGB.png"
        )
        im1 = Image(logo, 2 * inch, inch)
        im1.hAlign = "LEFT"

        # Add FZJ logo
        logo = os.path.join(cwd, "..", "..", "reports", "logos", "juelich_logo.png")
        im2 = Image(logo, 2 * inch, inch)
        im2.hAlign = "RIGHT"

        data = [[im1, im2]]
        t = Table(data)
        story.append(t)
        story.append(Spacer(1, 24))

        # Add report title
        ptext = '<font size="20">{}</font>'.format(report_title)
        story.append(Paragraph(ptext, self.styles["Title"]))
        story.append(Spacer(1, 100))

        # Add authors
        authors = [
            ["Authors:", "Shruthi Patil (s.patil@fz-juelich.de)"],
            ["", "Dr. Noah Pflugradt (n.pflugradt@fz-juelich.de)"],
        ]

        table_style = [
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]

        story.append(Table(authors, style=table_style))
        story.append(Spacer(1, 300))

        # Inserts time
        formatted_time = date.today()
        ptext = '<font size="12">%s</font>' % formatted_time
        story.append(Paragraph(ptext, self.styles["Normal_center"]))
        story.append(Spacer(1, 12))

        self.story = story
        self.insert_pagebreak()

    def insert_table_of_contents(self) -> None:
        """Add table of contents."""
        toc = tableofcontents.TableOfContents()
        self.story.append(toc)
        self.insert_pagebreak()

    def insert_title(self, title: str) -> None:
        """Add a title."""
        ptext = '<font size="12">{}</font>'.format(title)
        self.story.append(Paragraph(ptext, self.styles["Heading3"]))
        self.story.append(Spacer(1, 12))

    def insert_text(self, text: str) -> None:
        """Add given text to report."""
        ptext = '<font size="12">{}</font>'.format(text)
        self.story.append(Paragraph(ptext, self.styles["Normal"]))
        self.story.append(Spacer(1, 12))

    def insert_table(self, data_df: pd.DataFrame) -> None:
        """Create table from dataframe and add to report."""
        table_header = [
            data_df.columns[
                :,
            ]
            .values.astype(str)
            .tolist()
        ]
        table_data = data_df.values.tolist()
        data_list = table_header + table_data

        table_style = [
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONT", (0, 0), (-1, 0), "Times-Bold"),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.black),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.black),
        ]

        table = Table(data_list, style=table_style)

        self.story.append(table)
        self.story.append(Spacer(1, 12))

    def insert_pagebreak(self) -> None:
        """Add a pagebreak."""
        self.story.append(PageBreak())

    def close(self) -> None:
        """Close the report."""
        story = copy.deepcopy(self.story)
        self.doc.build(story)
