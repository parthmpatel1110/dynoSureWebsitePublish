---
title: "DynoSure LoggerV1 - Standalone CAN 2.0B Data Logger"
date: 2018-11-18T12:33:46+10:00
draft: false
featured: true
weight: 3
image: "images/LOGGGER_no_bg.png"
---

The DynoSure LoggerV1 is a standalone CAN bus data logger designed for the Indian automotive industry. It captures CAN 2.0 traffic to onboard storage in the standard **Vector ASC file format** - no proprietary software, no vendor lock-in.

<!--more-->

![DynoSure LoggerV1](/images/LOGGGER_no_bg.png)

---

# 💡 Why Did We Build the DynoSure LoggerV1?

While commercial CAN logging tools are available, many are prohibitively expensive for the Indian automotive sector or trap users in restrictive, closed ecosystems. We wanted to change that by building something **accessible** and **open**.

---

# Key Advantages

| Feature | Benefit |
|---|---|
| **Open Standards** | Logs in Vector ASC format - compatible with any CAN analysis tool |
| **Standalone Operation** | No PC required during logging - connect, power on, and capture |
| **Future-Proof** | User-end firmware updates for continuous improvements |
| **Cost-Effective** | Premium performance at a price point suited for the Indian market |

---

# Specifications

| Parameter | Details |
|---|---|
| **Supported CAN Protocols** | CAN 2.0A (11-bit Standard ID), CAN 2.0B (29-bit Extended ID) |
| **USB Interface** | USB 2.0 Full-Speed (backward compatible with USB 1.1, forward compatible with USB 3.0) |
| **Bitrate Configuration** | Configured via `configuration.txt` on the microSD card:<br>• `1` = 500 kbps (default)<br>• `2` = 1 Mbps<br>• `3` = 250 kbps |
| **Data Storage** | Onboard microSD card for offline CAN data capture |
| **Log Format** | Vector ASC (industry-standard) |
| **Logging Features** | Timestamped messages, configurable ID filters |
| **Power Supply & Operating Modes** | • **Logging Mode**: Requires **+12V DC** external power supply via DB9 connector (**Pin 9: +12V**, **Pin 3: GND**)<br>• **USB Mode**: USB powered - acts as a standard **microSD Card Reader** to copy logs to PC |
| **Firmware Update** | User-programmable (customer-end firmware update capability) |
| **Operating Temperature** | Extended range - suitable for industrial & automotive environments |

> **Note:** The LoggerV1 supports CAN 2.0A and CAN 2.0B protocols. CAN-FD is not supported on this product. For CAN-FD requirements, see our [SLCANv1](/services/product/) or [SLCAN GPIO](/services/slcangpio/) adapters.

---

# Downloads

| Resource | Link |
|---|---|
| 📄 Product Datasheet | [⬇️ Download](./../../files/DynoSure_Logger_Datasheet.pdf) |
| 🐍 Log to Vector ASC Converter | [⬇️ Download](./../../files/log2Asc.py) |
| ⚙️ ASC to Excel Converter | [⬇️ Download](./../../files/python_code_for_asc_excel.zip) |

---

# Trusted By

- Bgauss Pvt Ltd.
- RTCON Engineering
- Jindal Mobilitric Pvt Ltd.
- MATEL Motion and Energy Solutions Pvt Ltd.
- TRONTEK ELECTRONICS LIMITED
- Lord's Automative Private Limited

---

We are committed to providing affordable, open, and powerful diagnostic tools to keep you moving forward.

### 📞 **Ready to order?** Contact us at **+91 9898204057 (Mukesh Patel)** or **+91 9422556559**, or email **dynosure.india@gmail.com**
