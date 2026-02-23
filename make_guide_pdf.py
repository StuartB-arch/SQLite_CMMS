"""Generate AIT_CMMS_INSTALLATION_AND_USER_GUIDE.pdf from markdown content."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

OUTPUT = "AIT_CMMS_INSTALLATION_AND_USER_GUIDE.pdf"

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------
base = getSampleStyleSheet()

title_style = ParagraphStyle(
    "GuideTitle",
    parent=base["Title"],
    fontSize=22,
    leading=28,
    textColor=colors.HexColor("#1a3a5c"),
    spaceAfter=4,
)
subtitle_style = ParagraphStyle(
    "GuideSubtitle",
    parent=base["Normal"],
    fontSize=12,
    textColor=colors.HexColor("#3a5a7c"),
    spaceAfter=2,
    alignment=TA_CENTER,
)
h1_style = ParagraphStyle(
    "H1",
    parent=base["Heading1"],
    fontSize=15,
    leading=20,
    textColor=colors.white,
    backColor=colors.HexColor("#1a3a5c"),
    spaceBefore=14,
    spaceAfter=6,
    leftIndent=-6,
    rightIndent=-6,
    borderPad=4,
)
h2_style = ParagraphStyle(
    "H2",
    parent=base["Heading2"],
    fontSize=12,
    leading=16,
    textColor=colors.HexColor("#1a3a5c"),
    spaceBefore=10,
    spaceAfter=4,
    borderPad=2,
)
h3_style = ParagraphStyle(
    "H3",
    parent=base["Heading3"],
    fontSize=10,
    leading=14,
    textColor=colors.HexColor("#2a5a8c"),
    spaceBefore=8,
    spaceAfter=3,
    fontName="Helvetica-BoldOblique",
)
body_style = ParagraphStyle(
    "Body",
    parent=base["Normal"],
    fontSize=9.5,
    leading=14,
    spaceAfter=4,
)
bullet_style = ParagraphStyle(
    "Bullet",
    parent=body_style,
    leftIndent=18,
    bulletIndent=6,
    spaceAfter=2,
)
note_style = ParagraphStyle(
    "Note",
    parent=body_style,
    backColor=colors.HexColor("#fff8e1"),
    borderColor=colors.HexColor("#f0a500"),
    borderWidth=1,
    borderPad=6,
    leftIndent=8,
    rightIndent=8,
    spaceAfter=6,
)
code_style = ParagraphStyle(
    "Code",
    parent=body_style,
    fontName="Courier",
    fontSize=9,
    backColor=colors.HexColor("#f4f4f4"),
    borderColor=colors.HexColor("#cccccc"),
    borderWidth=1,
    borderPad=6,
    leftIndent=8,
    rightIndent=8,
    spaceAfter=6,
)
footer_style = ParagraphStyle(
    "Footer",
    parent=base["Normal"],
    fontSize=8,
    textColor=colors.grey,
    alignment=TA_CENTER,
)

# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------
HEADER_BG  = colors.HexColor("#1a3a5c")
ROW_ALT    = colors.HexColor("#eef3f8")
TABLE_FONT = 9

def make_table(headers, rows, col_widths=None):
    data = [[Paragraph(f"<b>{h}</b>", ParagraphStyle("th", parent=body_style,
             textColor=colors.white, fontSize=TABLE_FONT)) for h in headers]]
    for i, row in enumerate(rows):
        data.append([Paragraph(str(c), ParagraphStyle("td", parent=body_style,
                     fontSize=TABLE_FONT)) for c in row])
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, ROW_ALT]),
        ("GRID",       (0, 0), (-1, -1), 0.4, colors.HexColor("#b0c0d0")),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ])
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(style)
    return t

# ---------------------------------------------------------------------------
# Document content
# ---------------------------------------------------------------------------
def build():
    doc = SimpleDocTemplate(
        OUTPUT,
        pagesize=letter,
        leftMargin=0.85*inch,
        rightMargin=0.85*inch,
        topMargin=0.9*inch,
        bottomMargin=0.9*inch,
        title="AIT CMMS Installation & User Guide",
        author="AIT Maintenance",
    )

    W = doc.width  # usable width
    story = []

    # ── Cover block ──────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.4*inch))
    story.append(Paragraph("AIT CMMS", title_style))
    story.append(Paragraph("Installation &amp; User Guide", subtitle_style))
    story.append(Paragraph("AIT Complete CMMS — Computerized Maintenance Management System",
                            subtitle_style))
    story.append(Spacer(1, 0.1*inch))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor("#1a3a5c")))
    story.append(Spacer(1, 0.25*inch))

    # ── TABLE OF CONTENTS (static) ───────────────────────────────────────────
    story.append(Paragraph("TABLE OF CONTENTS", h1_style))
    toc_items = [
        "1.  What You Will Need",
        "2.  Step 1 — Install Python",
        "3.  Step 2 — Install Required Libraries",
        "4.  Step 3 — Set Up the Program Files",
        "5.  Step 4 — Run the Program",
        "6.  Logging In",
        "7.  Navigating the System",
        "8.  Common Tasks",
        "9.  Running Reports",
        "10. Backing Up Your Data",
        "11. Troubleshooting",
        "12. Quick Reference Card",
    ]
    for item in toc_items:
        story.append(Paragraph(item, bullet_style))
    story.append(Spacer(1, 0.15*inch))

    # =========================================================================
    # SECTION 1
    # =========================================================================
    story.append(Paragraph("1. What You Will Need", h1_style))
    for line in [
        "• A Windows PC (Windows 10 or 11 recommended)",
        "• Internet access — one time only, for installation",
        "• The <b>SQLite_CMMS-main</b> folder provided to you (received via email or download link)",
        "• No prior technical knowledge is required — follow each step in order",
    ]:
        story.append(Paragraph(line, bullet_style))

    # =========================================================================
    # SECTION 2
    # =========================================================================
    story.append(Paragraph("2. Step 1 — Install Python", h1_style))
    story.append(Paragraph(
        "Python is the language this program is written in. You only need to install it once.", body_style))
    story.append(Spacer(1, 4))

    steps = [
        ("1", "Open your web browser and go to: <b>https://www.python.org/downloads</b>"),
        ("2", 'Click the large yellow <b>\u201cDownload Python\u201d</b> button.'),
        ("3", "Once the file downloads, open it (double-click)."),
        ("4", "<b>IMPORTANT:</b> On the first screen of the installer, check the box that says "
              "<b>Add Python to PATH</b> — this box is near the bottom of the window. "
              "Do not skip this step."),
        ("5", "Click <b>Install Now</b> and wait for it to complete."),
        ("6", "Click <b>Close</b> when finished."),
    ]
    for num, text in steps:
        story.append(Paragraph(f"<b>Step {num}.</b> {text}", bullet_style))

    story.append(Spacer(1, 6))
    story.append(Paragraph("<b>To verify Python installed correctly:</b>", h3_style))
    for line in [
        "• Press the <b>Windows key</b>, type <font name='Courier'>cmd</font>, and press Enter.",
        "• In the black window, type: <font name='Courier'>python --version</font> and press Enter.",
        "• You should see something like: <font name='Courier'>Python 3.12.0</font>",
        "• If you see that, Python is installed correctly.",
    ]:
        story.append(Paragraph(line, bullet_style))

    # =========================================================================
    # SECTION 3
    # =========================================================================
    story.append(Paragraph("3. Step 2 — Install Required Libraries", h1_style))
    story.append(Paragraph(
        "The program needs a few additional components. This is done with one command.", body_style))
    story.append(Spacer(1, 4))

    for num, text in [
        ("1", "Press the <b>Windows key</b>, type <font name='Courier'>cmd</font>, and press Enter."),
        ("2", "Copy and paste the following command exactly, then press <b>Enter</b>:"),
    ]:
        story.append(Paragraph(f"<b>Step {num}.</b> {text}", bullet_style))

    story.append(Paragraph("pip install pandas reportlab openpyxl pillow", code_style))

    for num, text in [
        ("3", "You will see text scrolling — this is normal. Wait until it stops and you see the cursor again."),
        ("4", "Close the Command Prompt window."),
    ]:
        story.append(Paragraph(f"<b>Step {num}.</b> {text}", bullet_style))

    story.append(Paragraph(
        "&#9432; You only need to do this once. These libraries stay installed permanently.", note_style))

    # =========================================================================
    # SECTION 4
    # =========================================================================
    story.append(Paragraph("4. Step 3 — Set Up the Program Files", h1_style))

    for num, text in [
        ("1", "Locate the <b>SQLite_CMMS-main</b> folder from your email or download."),
        ("2", "Move or copy this folder somewhere easy to find — your <b>Desktop</b> or <b>C:\\AIT_CMMS\\</b> is ideal."),
        ("3", "Open the folder. Inside you will see a folder called <b>AIT_CMMS2.3.1</b> — open that too."),
        ("4", "You should see many files including one called <b>AIT_CMMS_REV3.py</b> — this is the program."),
    ]:
        story.append(Paragraph(f"<b>Step {num}.</b> {text}", bullet_style))

    story.append(Paragraph(
        "&#9888; Important: Do not move individual files out of the AIT_CMMS2.3.1 folder. "
        "All files must stay together for the program to work correctly.", note_style))

    # =========================================================================
    # SECTION 5
    # =========================================================================
    story.append(Paragraph("5. Step 4 — Run the Program", h1_style))
    story.append(Paragraph("<b>Every time you want to open the program:</b>", h3_style))
    for num, text in [
        ("1", "Open the <b>AIT_CMMS2.3.1</b> folder."),
        ("2", "Double-click the file named <b>AIT_CMMS_REV3.py</b>."),
        ("3", "The program will open within a few seconds."),
    ]:
        story.append(Paragraph(f"<b>Step {num}.</b> {text}", bullet_style))

    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Tip — Create a shortcut:</b> Right-click <font name='Courier'>AIT_CMMS_REV3.py</font> "
        "→ Send to → Desktop (create shortcut). You can then double-click that shortcut from your Desktop each time.",
        note_style))
    story.append(Paragraph(
        "<b>If double-clicking opens Notepad instead of running the program:</b> "
        "Right-click the file → Open with → Choose another app → select <b>Python</b> "
        "→ check 'Always use this app' → OK.",
        note_style))

    # =========================================================================
    # SECTION 6
    # =========================================================================
    story.append(Paragraph("6. Logging In", h1_style))
    story.append(Paragraph(
        "When the program opens, a login screen will appear. Use one of the accounts below:", body_style))
    story.append(Spacer(1, 6))

    story.append(make_table(
        ["Username", "Password", "Role"],
        [
            ["admin",   "admin123",   "Manager — full access"],
            ["manager", "manager123", "Manager — full access"],
            ["tech1",   "tech1",      "Technician"],
            ["apenson", "apenson",    "Parts Coordinator"],
        ],
        col_widths=[W*0.22, W*0.28, W*0.50],
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "&#9432; Security recommendation: Change passwords after first login via the user management settings.",
        note_style))

    # =========================================================================
    # SECTION 7
    # =========================================================================
    story.append(Paragraph("7. Navigating the System", h1_style))
    story.append(Paragraph(
        "Once logged in, the main window has several tabs across the top:", body_style))
    story.append(Spacer(1, 6))

    story.append(make_table(
        ["Tab", "Purpose"],
        [
            ["Dashboard", "Overview — equipment count, overdue PMs, recent activity"],
            ["Equipment", "View, add, edit, and manage all equipment/assets"],
            ["PM Schedule", "Weekly preventive maintenance assignments and schedule"],
            ["Corrective Maintenance (CM)", "Log and track repair work orders"],
            ["Parts / MRO", "Spare parts and stock inventory"],
            ["Manuals", "Equipment documentation and reference files"],
            ["Reports", "Generate and export monthly summary and other reports"],
            ["Users", "Manage user accounts (Manager role only)"],
            ["Backup", "Back up and restore the database"],
        ],
        col_widths=[W*0.32, W*0.68],
    ))

    # =========================================================================
    # SECTION 8
    # =========================================================================
    story.append(Paragraph("8. Common Tasks", h1_style))

    tasks = [
        ("Add New Equipment", [
            "Click the <b>Equipment</b> tab.",
            "Click <b>Add Equipment</b>.",
            "Fill in the fields: BFM Number, SAP Number, Description, Location, Priority.",
            "Select which PM types apply (Weekly / Monthly / Six-Month / Annual).",
            "Click <b>Save</b>.",
        ]),
        ("Record a Completed PM", [
            "Click the <b>PM Schedule</b> tab.",
            "Find the equipment in the list.",
            "Click <b>Record Completion</b> (or right-click the item for options).",
            "Enter the technician name, date, hours worked, and any notes.",
            "Click <b>Save</b>.",
        ]),
        ("Log a Corrective Maintenance Work Order", [
            "Click the <b>Corrective Maintenance</b> tab.",
            "Click <b>New CM</b>.",
            "Enter the equipment, description of the fault, and priority.",
            "Click <b>Save</b> — a CM number is automatically assigned.",
            "Update the status (Open → In Progress → Closed) as work progresses.",
        ]),
        ("Search for Equipment", [
            "Click the <b>Equipment</b> tab.",
            "Use the <b>Search</b> bar at the top to type a BFM number, description, or location.",
            "The list filters as you type.",
        ]),
        ("Change Your Password", [
            "Click your username or go to the <b>Users</b> menu.",
            "Select <b>Change Password</b>.",
            "Enter your current password, then your new password twice.",
            "Click <b>Save</b>.",
        ]),
    ]

    for task_title, task_steps in tasks:
        block = [Paragraph(task_title, h2_style)]
        for i, step in enumerate(task_steps, 1):
            block.append(Paragraph(f"{i}. {step}", bullet_style))
        block.append(Spacer(1, 4))
        story.append(KeepTogether(block))

    # =========================================================================
    # SECTION 9
    # =========================================================================
    story.append(Paragraph("9. Running Reports", h1_style))

    reports = [
        ("Monthly Summary Report", [
            "Click the <b>Reports</b> tab (or use the menu bar).",
            "Select <b>Monthly PM Summary</b>.",
            "Choose the <b>Month</b> and <b>Year</b> from the dropdowns.",
            "Click <b>Generate Report</b> or <b>Export</b>.",
            "Choose where to save the file — it will be saved as a <font name='Courier'>.csv</font> or <font name='Courier'>.xlsx</font> file.",
        ]),
        ("Equipment List Export", [
            "Click the <b>Equipment</b> tab.",
            "Click <b>Export</b> or <b>Export to CSV</b>.",
            "Choose a save location.",
            "Open the file in Excel.",
        ]),
        ("Corrective Maintenance Report", [
            "Click the <b>Corrective Maintenance</b> tab.",
            "Click <b>Export Report</b>.",
            "Choose the format: <b>CSV</b>, <b>Excel</b>, or <b>PDF</b>.",
            "Choose a save location and click <b>Save</b>.",
        ]),
    ]

    for rep_title, rep_steps in reports:
        block = [Paragraph(rep_title, h2_style)]
        for i, step in enumerate(rep_steps, 1):
            block.append(Paragraph(f"{i}. {step}", bullet_style))
        block.append(Spacer(1, 4))
        story.append(KeepTogether(block))

    # =========================================================================
    # SECTION 10
    # =========================================================================
    story.append(Paragraph("10. Backing Up Your Data", h1_style))
    story.append(Paragraph(
        "All your data is stored in a single database file (<font name='Courier'>.db</font>) "
        "inside the <b>AIT_CMMS2.3.1</b> folder. Back it up regularly to avoid data loss.",
        body_style))

    story.append(Paragraph("Using the Built-In Backup Tool", h2_style))
    for i, step in enumerate([
        "Click the <b>Backup</b> tab.",
        "Click <b>Create Backup</b>.",
        "Choose a destination folder (a USB drive or cloud folder like OneDrive/Google Drive is ideal).",
        "Click <b>Save</b>.",
    ], 1):
        story.append(Paragraph(f"{i}. {step}", bullet_style))

    story.append(Paragraph("Manual Backup (Simple Method)", h2_style))
    for i, step in enumerate([
        "Close the program first.",
        "Open the <b>AIT_CMMS2.3.1</b> folder.",
        "Find the file ending in <font name='Courier'>.db</font> — this is your entire database.",
        "Copy it to a USB drive, OneDrive, or Google Drive folder.",
    ], 1):
        story.append(Paragraph(f"{i}. {step}", bullet_style))

    story.append(Paragraph(
        "&#9888; Recommendation: Back up weekly at minimum. Store at least one copy off-site or in cloud storage.",
        note_style))

    # =========================================================================
    # SECTION 11
    # =========================================================================
    story.append(Paragraph("11. Troubleshooting", h1_style))

    issues = [
        (
            "The program doesn't open when I double-click the file",
            [
                "Make sure Python is installed (see Step 1).",
                "Right-click the <font name='Courier'>.py</font> file → Open with → Python.",
                "If Python is not listed, re-install Python and check \"Add to PATH\".",
            ]
        ),
        (
            "I see an error about a missing module",
            [
                "Open Command Prompt and run:",
                "<font name='Courier'>pip install pandas reportlab openpyxl pillow</font>",
                "Then try opening the program again.",
            ]
        ),
        (
            "The program opens but then crashes immediately",
            [
                "Make sure all files are still inside the <b>AIT_CMMS2.3.1</b> folder — nothing should be moved out.",
                "Check that the <font name='Courier'>.db</font> database file is in the same folder as <font name='Courier'>AIT_CMMS_REV3.py</font>.",
            ]
        ),
        (
            "I forgot my password",
            [
                "Contact your system administrator to reset it via the <b>Users</b> tab (Manager role required).",
            ]
        ),
        (
            "The program is running slowly",
            [
                "Close other programs to free up memory.",
                "Use the search/filter to narrow results before loading large equipment lists.",
            ]
        ),
        (
            "I accidentally deleted something",
            [
                "Go to the Backup tab and restore from the most recent backup.",
                "This is why regular backups are strongly recommended.",
            ]
        ),
    ]

    for issue_title, issue_steps in issues:
        block = [Paragraph(issue_title, h2_style)]
        for step in issue_steps:
            block.append(Paragraph(f"• {step}", bullet_style))
        block.append(Spacer(1, 4))
        story.append(KeepTogether(block))

    # =========================================================================
    # SECTION 12 — Quick Reference
    # =========================================================================
    story.append(Paragraph("12. Quick Reference Card", h1_style))
    story.append(make_table(
        ["Task", "Where to Go"],
        [
            ["Open the program",         "Double-click AIT_CMMS_REV3.py"],
            ["Add equipment",            "Equipment tab → Add Equipment"],
            ["Record PM completion",     "PM Schedule tab → Record Completion"],
            ["Log a repair job",         "Corrective Maintenance tab → New CM"],
            ["Run monthly report",       "Reports tab → Monthly PM Summary"],
            ["Export equipment list",    "Equipment tab → Export"],
            ["Back up data",             "Backup tab → Create Backup"],
            ["Change password",          "Users menu → Change Password"],
        ],
        col_widths=[W*0.45, W*0.55],
    ))

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.3*inch))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#1a3a5c")))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "AIT Complete CMMS — Computerized Maintenance Management System<br/>"
        "For technical support, contact your system administrator.",
        footer_style))

    # ── Build ─────────────────────────────────────────────────────────────────
    doc.build(story)
    print(f"PDF created: {OUTPUT}")


if __name__ == "__main__":
    build()
