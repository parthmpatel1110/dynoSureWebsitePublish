import os
from PIL import Image as PILImage
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY

# Target Directory
output_dir = "d:/project_impossible/websiteGithub/content/files"
os.makedirs(output_dir, exist_ok=True)

# Styles
styles = getSampleStyleSheet()

title_style = ParagraphStyle(
    'DocTitle',
    parent=styles['Heading1'],
    fontName='Helvetica-Bold',
    fontSize=18,
    leading=22,
    textColor=colors.HexColor('#2F2F41'),
    spaceAfter=15
)

section_style = ParagraphStyle(
    'SectionHeader',
    parent=styles['Heading2'],
    fontName='Helvetica-Bold',
    fontSize=11,
    leading=13,
    textColor=colors.HexColor('#865E5E'),
    spaceBefore=8,
    spaceAfter=5,
    keepWithNext=True
)

body_style = ParagraphStyle(
    'BodyTextCustom',
    parent=styles['BodyText'],
    fontName='Helvetica',
    fontSize=8.5,
    leading=11.5,
    textColor=colors.HexColor('#333333'),
    spaceAfter=6
)

table_header_style = ParagraphStyle(
    'TableHeader',
    parent=styles['Normal'],
    fontName='Helvetica-Bold',
    fontSize=8,
    leading=10,
    textColor=colors.white
)

table_cell_style = ParagraphStyle(
    'TableCell',
    parent=styles['Normal'],
    fontName='Helvetica',
    fontSize=7.5,
    leading=10,
    textColor=colors.HexColor('#333333')
)

table_cell_bold_style = ParagraphStyle(
    'TableCellBold',
    parent=styles['Normal'],
    fontName='Helvetica-Bold',
    fontSize=7.5,
    leading=10,
    textColor=colors.HexColor('#333333')
)

# Cover Styles
cover_brand_style = ParagraphStyle(
    'CoverBrand',
    parent=styles['Normal'],
    fontName='Helvetica-Bold',
    fontSize=16,
    leading=18,
    textColor=colors.HexColor('#D4AF37'), # Gold
    spaceAfter=6
)

cover_title_style = ParagraphStyle(
    'CoverTitle',
    parent=styles['Heading1'],
    fontName='Helvetica-Bold',
    fontSize=26,
    leading=30,
    textColor=colors.white,
    spaceAfter=8
)

cover_subtitle_style = ParagraphStyle(
    'CoverSubtitle',
    parent=styles['Normal'],
    fontName='Helvetica',
    fontSize=12,
    leading=15,
    textColor=colors.white,
    spaceAfter=15
)

cover_desc_style = ParagraphStyle(
    'CoverDesc',
    parent=styles['Normal'],
    fontName='Helvetica-Bold',
    fontSize=10,
    leading=14,
    textColor=colors.HexColor('#2F2F41'),
    alignment=TA_CENTER
)

def draw_single_page_decorations(canvas, doc):
    canvas.saveState()
    # Header Band
    canvas.setFillColor(colors.HexColor('#865E5E')) # Primary
    canvas.rect(0, 800, 595.27, 42, fill=True, stroke=False)
    
    canvas.setFillColor(colors.white)
    canvas.setFont('Helvetica-Bold', 11)
    canvas.drawString(36, 814, "DynoSure Automotive CAN Bus Tools")
    canvas.setFont('Helvetica', 8)
    canvas.drawRightString(559, 816, "PRODUCT TECHNICAL DATASHEET")
    
    # Gold accent stripe
    canvas.setFillColor(colors.HexColor('#D4AF37'))
    canvas.rect(0, 797, 595.27, 3, fill=True, stroke=False)
    
    # Footer
    canvas.setFillColor(colors.HexColor('#2F2F41'))
    canvas.rect(0, 0, 595.27, 30, fill=True, stroke=False)
    
    canvas.setFillColor(colors.white)
    canvas.setFont('Helvetica', 8)
    canvas.drawString(36, 11, "© 2026 Maruti Electrical & Electronics. All rights reserved.")
    canvas.drawRightString(559, 11, "www.dynosure.co.in")
    canvas.restoreState()

def draw_catalog_decorations(canvas, doc):
    canvas.saveState()
    if doc.page == 1:
        # Cover page dark top half
        canvas.setFillColor(colors.HexColor('#2F2F41'))
        canvas.rect(0, 480, 595.27, 362.89, fill=True, stroke=False)
        
        # Gold accent stripe
        canvas.setFillColor(colors.HexColor('#D4AF37'))
        canvas.rect(0, 475, 595.27, 5, fill=True, stroke=False)
        
        # Bottom cover footer banner
        canvas.setFillColor(colors.HexColor('#865E5E'))
        canvas.rect(0, 0, 595.27, 45, fill=True, stroke=False)
        
        canvas.setFillColor(colors.white)
        canvas.setFont('Helvetica-Bold', 11)
        canvas.drawString(36, 18, "Maruti Electrical & Electronics")
        canvas.drawRightString(559, 18, "PRODUCT CATALOG - 2026")
    else:
        # Standard page header
        canvas.setFillColor(colors.HexColor('#865E5E'))
        canvas.rect(0, 800, 595.27, 42, fill=True, stroke=False)
        
        canvas.setFillColor(colors.white)
        canvas.setFont('Helvetica-Bold', 11)
        canvas.drawString(36, 814, "DynoSure Automotive CAN Bus Tools")
        canvas.setFont('Helvetica', 8)
        canvas.drawRightString(559, 816, f"CATALOG PAGE {doc.page}")
        
        # Gold accent stripe
        canvas.setFillColor(colors.HexColor('#D4AF37'))
        canvas.rect(0, 797, 595.27, 3, fill=True, stroke=False)
        
        # Footer
        canvas.setFillColor(colors.HexColor('#2F2F41'))
        canvas.rect(0, 0, 595.27, 30, fill=True, stroke=False)
        
        canvas.setFillColor(colors.white)
        canvas.setFont('Helvetica', 8)
        canvas.drawString(36, 11, "© 2026 Maruti Electrical & Electronics. All rights reserved.")
        canvas.drawRightString(559, 11, f"Page {doc.page} | www.dynosure.co.in")
    canvas.restoreState()

def make_table(headers, data, col_widths):
    formatted_data = []
    formatted_data.append([Paragraph(h, table_header_style) for h in headers])
    for row in data:
        formatted_row = []
        for cell in row:
            if isinstance(cell, Paragraph):
                formatted_row.append(cell)
            elif cell.startswith("**") and cell.endswith("**"):
                formatted_row.append(Paragraph(cell.replace("**", ""), table_cell_bold_style))
            else:
                formatted_row.append(Paragraph(cell, table_cell_style))
        formatted_data.append(formatted_row)
    
    t = Table(formatted_data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#865E5E')),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 5),
        ('RIGHTPADDING', (0,0), (-1,-1), 5),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F6F7FF')]),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#E8E8E8')),
    ]))
    return t

def make_aspect_image(img_path, target_width, align='CENTER'):
    if not os.path.exists(img_path):
        return Spacer(1, 1)
    with PILImage.open(img_path) as img:
        w, h = img.size
    aspect = h / w
    target_height = target_width * aspect
    return Image(img_path, width=target_width, height=target_height, hAlign=align)

# ----------------- 1. SLCANv1 Datasheet -----------------
def generate_slcanv1():
    filename = f"{output_dir}/DynoSure_USB_CAN_Adapter.pdf"
    doc = SimpleDocTemplate(filename, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=60, bottomMargin=45)
    story = []
    
    left_flow = []
    left_flow.append(Paragraph("DynoSure SLCANv1", title_style))
    left_flow.append(Paragraph("<b>Model:</b> SLCANv1 (USB-to-CAN Adapter)", body_style))
    left_flow.append(Paragraph("The DynoSure SLCANv1 adapter provides a reliable and convenient connection between a PC and a CAN (Controller Area Network) bus. Based on the open-source CANable2 firmware and the Lawicel SLCAN protocol, it exposes the CAN interface as a standard virtual COM port, simplifying integration with existing tools and reducing software overhead.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data = [
        ("Microcontroller", "STM32G4 Series, 170 MHz"),
        ("CAN Protocols", "CAN 2.0A (11-bit), CAN 2.0B (29-bit), CAN-FD"),
        ("USB Interface", "USB 2.0 Full-Speed (compatible with USB 1.1 / 3.0)"),
        ("Standard Bitrates", "5 kbps to 1 Mbps"),
        ("CAN-FD Bitrates", "Up to 8 Mbps data phase bitrates"),
        ("Power Supply", "USB-powered (no external supply required)"),
        ("Compatibility", "Windows & Linux (exposes standard Virtual COM Port)"),
        ("Software Support", "Compatible with BusMaster, Python, and C++ SDKs"),
        ("Operating Temp", "Extended range suitable for industrial environments"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data, [100, 205]))
    
    right_flow = []
    img_path = "d:/project_impossible/websiteGithub/static/images/Slcanv1_no_bg.png"
    right_flow.append(make_aspect_image(img_path, 180))
    right_flow.append(Spacer(1, 10))
    
    right_flow.append(Paragraph("DB9 Pinout Assignment", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data = [
        ("Pin 1", "Not Connected"),
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Ground)"),
        ("Pin 4", "Not Connected"),
        ("Pin 5", "Not Connected"),
        ("Pin 6", "<b>GND</b> (Ground, secondary)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Pin 8", "Not Connected"),
        ("Pin 9", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data, [50, 150]))
    
    right_flow.append(Spacer(1, 10))
    right_flow.append(Paragraph("<b>Inquiries & Ordering:</b><br/>Email: <b>dynosure.india@gmail.com</b><br/>Mobile: <b>+91 9898204057 (Mukesh Patel)</b><br/>Mobile 2: <b>+91 9422556559 </b>", body_style))
    
    col_table = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table)
    
    doc.build(story, onFirstPage=draw_single_page_decorations, onLaterPages=draw_single_page_decorations)

# ----------------- 2. SLCAN GPIO Datasheet -----------------
def generate_slcan_gpio():
    filename = f"{output_dir}/DynoSure_SLCAN_GPIO_Datasheet.pdf"
    doc = SimpleDocTemplate(filename, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=60, bottomMargin=45)
    story = []
    
    left_flow = []
    left_flow.append(Paragraph("DynoSure SLCAN GPIO", title_style))
    left_flow.append(Paragraph("<b>Model:</b> SLCAN GPIO (USB-to-CAN with 8-ch GPIO Control)", body_style))
    left_flow.append(Paragraph("The DynoSure SLCAN GPIO combines a full-featured USB-to-CAN adapter with 8 configurable GPIO outputs. This gives hardware developers and test engineers both CAN bus communication and physical hardware control in a single compact device. Exposes CAN standard interfaces and digital outputs directly through internal messaging controls.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data = [
        ("Microcontroller", "STM32G4 Series, 170 MHz"),
        ("CAN Protocols", "CAN 2.0A (11-bit), CAN 2.0B (29-bit), CAN-FD"),
        ("USB Interface", "USB 2.0 Full-Speed"),
        ("GPIO Channels", "8 Configurable Digital Outputs"),
        ("Power Supply", "USB-powered (no external supply required)"),
        ("Compatibility", "Windows & Linux (Virtual COM Port)"),
        ("Software Support", "BusMaster, Python, C++ SDKs"),
        ("GPIO Control", "Exposes GPIO via internal CAN message commands"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data, [95, 210]))
    
    left_flow.append(Paragraph("GPIO Control Protocol", section_style))
    left_flow.append(Paragraph("The 8 GPIO outputs are controlled by sending a special CAN command message. This message is processed internally by the firmware and is <b>not</b> transmitted on the physical CAN bus.", body_style))
    
    proto_data = [
        ("CAN ID", "<b>0x1FFFFF</b> (Extended 29-bit ID)"),
        ("DLC", "<b>2</b>"),
        ("Byte 0", "GPIO States (Bit 0 = GPIO 0 ... Bit 7 = GPIO 7). 1 = HIGH, 0 = LOW"),
        ("Byte 1", "<b>0xAA</b> (Fixed GPIO command identifier)"),
    ]
    left_flow.append(make_table(["Field", "Value / Description"], proto_data, [50, 255]))
    
    right_flow = []
    img_path = "d:/project_impossible/websiteGithub/static/images/SLCAN_GPIO_no_bg.png"
    right_flow.append(make_aspect_image(img_path, 180))
    right_flow.append(Spacer(1, 5))
    
    right_flow.append(Paragraph("DB9 CAN Pinout", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data_2 = [
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Ground)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Pin 6", "<b>GND</b> (Ground)"),
        ("Others", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data_2, [50, 150]))
    
    right_flow.append(Paragraph("GPIO Auxiliary Port Pinout", section_style))
    gpio_pin_data = [
        ("Pin 1", "<b>VCC (+5V Out)</b>"),
        ("Pin 2-9", "<b>GPIO 0 to GPIO 7</b>"),
        ("Pin 10", "<b>GND</b> (Ground)"),
    ]
    right_flow.append(make_table(["Aux Pin", "Assignment"], gpio_pin_data, [50, 150]))
    
    right_flow.append(Spacer(1, 5))
    right_flow.append(Paragraph("<b>Inquiries & Ordering:</b><br/>Email: <b>dynosure.india@gmail.com</b><br/>Mobile: <b>+91 9898204057 (Mukesh Patel)</b>", body_style))
    
    col_table = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table)
    
    doc.build(story, onFirstPage=draw_single_page_decorations, onLaterPages=draw_single_page_decorations)

# ----------------- 3. LoggerV1 Datasheet -----------------
def generate_logger():
    filename = f"{output_dir}/DynoSure_Logger_Datasheet.pdf"
    doc = SimpleDocTemplate(filename, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=60, bottomMargin=45)
    story = []
    
    left_flow = []
    left_flow.append(Paragraph("DynoSure LoggerV1", title_style))
    left_flow.append(Paragraph("<b>Model:</b> LoggerV1 (Standalone CAN Data Logger)", body_style))
    left_flow.append(Paragraph("The DynoSure LoggerV1 is a standalone CAN bus data logger designed specifically to capture CAN 2.0 traffic to onboard storage without requiring a connected PC during operations. It logs data in the industry-standard Vector ASC file format, avoiding restrictive closed ecosystems and vendor lock-in.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data = [
        ("CAN Protocols", "CAN 2.0A (11-bit ID), CAN 2.0B (29-bit extended ID)"),
        ("Storage", "Onboard microSD card (FAT32 filesystem)"),
        ("Log Format", "Vector ASC (standard ASCII format)"),
        ("USB Interface", "USB 2.0 (Mass Storage Device / Card Reader mode)"),
        ("Power Modes", "• <b>Logging Mode</b>: Requires <b>+12V DC</b> external supply<br/>• <b>USB Mode</b>: USB-powered (acts as SD card reader)"),
        ("Firmware Update", "User-programmable at customer end"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data, [90, 220]))
    
    left_flow.append(Paragraph("Baud Rate Configuration", section_style))
    left_flow.append(Paragraph("Bitrates are selected by creating a simple <code>configuration.txt</code> file in the root of the microSD card. The file should contain one of the following numbers:", body_style))
    
    bitrate_data = [
        ("Value", "Bitrate Selected"),
        ("<b>1</b>", "<b>500 kbps</b> (Standard default)"),
        ("<b>2</b>", "<b>1 Mbps</b> (High speed CAN)"),
        ("<b>3</b>", "<b>250 kbps</b> (Medium speed CAN)"),
    ]
    left_flow.append(make_table(["Key Value", "Resulting CAN Bus Speed"], bitrate_data[1:], [60, 245]))
    
    right_flow = []
    img_path = "d:/project_impossible/websiteGithub/static/images/LOGGGER_no_bg.png"
    right_flow.append(make_aspect_image(img_path, 180))
    right_flow.append(Spacer(1, 10))
    
    right_flow.append(Paragraph("DB9 CAN & Power Pinout", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data_3 = [
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Power Ground)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Pin 9", "<b>+12V DC</b> (Power Input)"),
        ("Others", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data_3, [50, 150]))
    
    right_flow.append(Spacer(1, 15))
    right_flow.append(Paragraph("<b>Operation Modes Summary:</b><br/>• <b>Logging Mode</b>: Insert card, connect +12V power supply via Pin 9 and Pin 3 on DB9. Device starts logging automatically.<br/>• <b>USB Mode</b>: Plug into PC via USB cable. Behaves as an external card reader to copy logs.", body_style))
    
    right_flow.append(Spacer(1, 10))
    right_flow.append(Paragraph("<b>Inquiries & Ordering:</b><br/>Email: <b>dynosure.india@gmail.com</b><br/>Mobile: <b>+91 9898204057 (Mukesh Patel)</b>", body_style))
    
    col_table = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table)
    
    doc.build(story, onFirstPage=draw_single_page_decorations, onLaterPages=draw_single_page_decorations)

# ----------------- 4. Combined Product Catalog -----------------
def generate_catalog():
    filename = f"{output_dir}/DynoSure_Product_Catalog.pdf"
    doc = SimpleDocTemplate(filename, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=60, bottomMargin=45)
    story = []
    
    # --- PAGE 1: COVER ---
    story.append(Spacer(1, 40))
    story.append(Paragraph("DynoSure", cover_brand_style))
    story.append(Paragraph("AUTOMOTIVE CAN BUS SOLUTIONS", cover_title_style))
    story.append(Paragraph("Comprehensive Product Catalog & Specifications Guide", cover_subtitle_style))
    
    story.append(Spacer(1, 100)) # Space to push image to white area
    
    img_path = "d:/project_impossible/websiteGithub/static/images/dynosure_all_products_no_bg.png"
    story.append(make_aspect_image(img_path, 380))
    story.append(Spacer(1, 20))
    
    story.append(Paragraph("Affordable, reliable diagnostic and data logging hardware engineered specifically for the Indian automotive sector. Based on open-source standards with vendor lock-in free integrations.", cover_desc_style))
    
    story.append(Spacer(1, 20))
    story.append(Paragraph("<font color='#555'><b>DynoSure India</b><br/>Vadodara, Gujarat, India | Email: dynosure.india@gmail.com | Web: www.dynosure.co.in</font>", ParagraphStyle('CoverContact', parent=styles['Normal'], alignment=TA_CENTER, fontSize=8.5, leading=12)))
    
    story.append(PageBreak())
    
    # --- PAGE 2: SLCANv1 ---
    left_flow = []
    left_flow.append(Paragraph("DynoSure SLCANv1", title_style))
    left_flow.append(Paragraph("<b>Model:</b> SLCANv1 (USB-to-CAN Adapter)", body_style))
    left_flow.append(Paragraph("The DynoSure SLCANv1 adapter provides a reliable and convenient connection between a PC and a CAN bus. Based on the open-source CANable2 firmware and the Lawicel SLCAN protocol, it exposes the CAN interface as a standard virtual COM port, simplifying integration with existing tools and reducing software overhead.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data_1 = [
        ("Microcontroller", "STM32G4 Series, 170 MHz"),
        ("CAN Protocols", "CAN 2.0A (11-bit), CAN 2.0B (29-bit), CAN-FD"),
        ("USB Interface", "USB 2.0 Full-Speed (compatible with USB 1.1 / 3.0)"),
        ("Standard Bitrates", "5 kbps to 1 Mbps"),
        ("CAN-FD Bitrates", "Up to 8 Mbps data phase bitrates"),
        ("Power Supply", "USB-powered (no external supply required)"),
        ("Compatibility", "Windows & Linux (exposes standard Virtual COM Port)"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data_1, [90, 215]))
    
    right_flow = []
    img1 = "d:/project_impossible/websiteGithub/static/images/Slcanv1_no_bg.png"
    right_flow.append(make_aspect_image(img1, 180))
    right_flow.append(Spacer(1, 10))
    
    right_flow.append(Paragraph("DB9 Pinout Assignment", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data_1 = [
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Ground)"),
        ("Pin 6", "<b>GND</b> (Ground, secondary)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Others", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data_1, [50, 150]))
    
    col_table1 = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table1.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table1)
    
    story.append(PageBreak())
    
    # --- PAGE 3: SLCAN GPIO ---
    left_flow = []
    left_flow.append(Paragraph("DynoSure SLCAN GPIO", title_style))
    left_flow.append(Paragraph("<b>Model:</b> SLCAN GPIO (USB-to-CAN with 8-ch GPIO Control)", body_style))
    left_flow.append(Paragraph("The DynoSure SLCAN GPIO combines a full-featured USB-to-CAN adapter with 8 configurable GPIO outputs. This gives hardware developers and test engineers both CAN bus communication and physical hardware control in a single compact device. Exposes CAN standard interfaces and digital outputs directly through internal messaging controls.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data_2 = [
        ("Microcontroller", "STM32G4 Series, 170 MHz"),
        ("CAN Protocols", "CAN 2.0A (11-bit), CAN 2.0B (29-bit), CAN-FD"),
        ("USB Interface", "USB 2.0 Full-Speed"),
        ("GPIO Channels", "8 Configurable Digital Outputs"),
        ("Power Supply", "USB-powered (no external supply required)"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data_2, [90, 215]))
    
    left_flow.append(Paragraph("GPIO Control Protocol", section_style))
    proto_data = [
        ("CAN ID", "<b>0x1FFFFF</b> (Extended 29-bit ID)"),
        ("DLC", "<b>2</b>"),
        ("Byte 0", "GPIO States (Bit 0 = GPIO 0 ... Bit 7 = GPIO 7). 1 = HIGH, 0 = LOW"),
        ("Byte 1", "<b>0xAA</b> (Fixed GPIO command identifier)"),
    ]
    left_flow.append(make_table(["Field", "Value / Description"], proto_data, [50, 255]))
    
    right_flow = []
    img2 = "d:/project_impossible/websiteGithub/static/images/SLCAN_GPIO_no_bg.png"
    right_flow.append(make_aspect_image(img2, 180))
    right_flow.append(Spacer(1, 10))
    
    right_flow.append(Paragraph("DB9 CAN Pinout", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data_2 = [
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Ground)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Others", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data_2, [50, 150]))
    
    col_table2 = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table2.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table2)
    
    story.append(PageBreak())
    
    # --- PAGE 4: LOGGER ---
    left_flow = []
    left_flow.append(Paragraph("DynoSure LoggerV1", title_style))
    left_flow.append(Paragraph("<b>Model:</b> LoggerV1 (Standalone CAN Data Logger)", body_style))
    left_flow.append(Paragraph("The DynoSure LoggerV1 is a standalone CAN bus data logger designed specifically to capture CAN 2.0 traffic to onboard storage without requiring a connected PC during operations. It logs data in the industry-standard Vector ASC file format, avoiding restrictive closed ecosystems and vendor lock-in.", body_style))
    
    left_flow.append(Paragraph("Specifications", section_style))
    specs_data_3 = [
        ("CAN Protocols", "CAN 2.0A (11-bit ID), CAN 2.0B (29-bit extended ID)"),
        ("Storage", "Onboard microSD card (FAT32 filesystem)"),
        ("Log Format", "Vector ASC (standard ASCII format)"),
        ("USB Interface", "USB 2.0 (Mass Storage Device / Card Reader mode)"),
        ("Power Modes", "• <b>Logging Mode</b>: Requires <b>+12V DC</b> external supply<br/>• <b>USB Mode</b>: USB-powered (acts as SD card reader)"),
        ("Firmware Update", "User-programmable at customer end"),
    ]
    left_flow.append(make_table(["Parameter", "Details"], specs_data_3, [90, 215]))
    
    left_flow.append(Paragraph("Baud Rate Configuration", section_style))
    bitrate_data = [
        ("Value", "Bitrate Selected"),
        ("<b>1</b>", "<b>500 kbps</b> (Standard default)"),
        ("<b>2</b>", "<b>1 Mbps</b> (High speed CAN)"),
        ("<b>3</b>", "<b>250 kbps</b> (Medium speed CAN)"),
    ]
    left_flow.append(make_table(["Key Value", "Resulting CAN Bus Speed"], bitrate_data[1:], [60, 245]))
    
    right_flow = []
    img3 = "d:/project_impossible/websiteGithub/static/images/LOGGGER_no_bg.png"
    right_flow.append(make_aspect_image(img3, 180))
    right_flow.append(Spacer(1, 10))
    
    right_flow.append(Paragraph("DB9 CAN & Power Pinout", section_style))
    right_flow.append(make_aspect_image("d:/project_impossible/websiteGithub/static/images/db9_connector.png", 90))
    right_flow.append(Spacer(1, 5))
    pinout_data_3 = [
        ("Pin 2", "<b>CAN-L</b> (CAN Low)"),
        ("Pin 3", "<b>GND</b> (Power Ground)"),
        ("Pin 7", "<b>CAN-H</b> (CAN High)"),
        ("Pin 9", "<b>+12V DC</b> (Power Input)"),
        ("Others", "Not Connected"),
    ]
    right_flow.append(make_table(["DB9 Pin", "Assignment"], pinout_data_3, [50, 150]))
    
    col_table3 = Table([[left_flow, right_flow]], colWidths=[310, 210])
    col_table3.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(col_table3)
    
    doc.build(story, onFirstPage=draw_catalog_decorations, onLaterPages=draw_catalog_decorations)

if __name__ == '__main__':
    print("Generating SLCANv1 Datasheet...")
    generate_slcanv1()
    print("Generating SLCAN GPIO Datasheet...")
    generate_slcan_gpio()
    print("Generating LoggerV1 Datasheet...")
    generate_logger()
    print("Generating Product Catalog...")
    generate_catalog()
    print("All PDFs generated successfully!")
