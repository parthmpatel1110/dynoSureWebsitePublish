---
title: "DynoSure SLCAN GPIO - USB to CAN Adapter with GPIO Control"
date: 2018-11-18T12:33:46+10:00
draft: false
featured: true
weight: 2
image: "images/SLCAN_GPIO_no_b.jpg"
---

The DynoSure SLCAN GPIO combines a full-featured USB-to-CAN adapter with **8 configurable GPIO outputs**, giving you CAN bus communication and hardware control in a single compact device. Built on the CANable2 firmware with the **Lawicel SLCAN protocol**, it appears as a virtual COM port and integrates seamlessly with BUSMASTER and other CAN tools.

<!--more-->

![DynoSure SLCAN GPIO](/images/SLCAN_GPIO_no_b.jpg)

---

# Specifications

| Parameter | Details |
|---|---|
| **Microcontroller** | STM32G4 series @ 170 MHz |
| **CAN Protocols** | CAN 2.0A (11-bit), CAN 2.0B (29-bit), CAN-FD |
| **USB Interface** | USB 2.0 Full-Speed (backward compatible with USB 1.1, forward compatible with USB 3.0) |
| **Standard CAN Bitrates** | 10, 20, 50, 83.3, 100, 125, 250, 500, 750 kbps, 1 Mbps |
| **CAN-FD Data Bitrates** | Up to 8 Mbps |
| **GPIO Channels** | 8 digital outputs |
| **Power Supply** | USB powered - no external supply required |
| **Operating Temperature** | Extended range - suitable for industrial & automotive environments |

---

# GPIO Control Protocol

The 8 GPIO outputs are controlled by sending a special CAN message through the adapter. This message is processed internally and is **not transmitted on the physical CAN bus**.

| Parameter | Value |
|---|---|
| **CAN ID** | `0x1FFFFF` (Extended 29-bit) |
| **DLC** | 2 |
| **Byte 0** | GPIO control - each bit maps to one GPIO pin (bit 0 → GPIO 0, bit 7 → GPIO 7). Set `1` = HIGH, `0` = LOW |
| **Byte 1** | `0xAA` (fixed identifier for GPIO command) |

### Example

To set GPIO 0, GPIO 2, and GPIO 4 HIGH (and all others LOW):

| Byte | Value | Binary |
|---|---|---|
| Byte 0 | `0x15` | `0001 0101` |
| Byte 1 | `0xAA` | `1010 1010` |

> **Note:** The GPIO control message (ID `0x1FFFFF`) is intercepted by the adapter firmware and will NOT appear on the physical CAN bus. All other CAN messages transmit normally.

---

# Software & Platform Support

- Enumerates as a **Virtual COM Port** - no custom drivers required
- Fully compatible with **[BUSMASTER](./../../files/BUSMASTER_Installer_Ver_3.2.2.exe)** - industry-standard CAN analysis tool
- Works with our **[DynoSure Python Library](https://pypi.org/project/dynosure-slcanv1/)** (available via PyPI), **python-can**, **C/C++** libraries, and any SLCAN-compatible software
- GPIO control works through the same COM port - no additional interface needed
- Supported on **Windows** and **Linux**

---

# Downloads

| Resource | Link |
|---|---|
| 📄 Product Datasheet | [⬇️ Download](./../../files/DynoSure_SLCAN_GPIO_Datasheet.pdf) |
| 🖥️ BUSMASTER Installer | [⬇️ Download](./../../files/BUSMASTER_Installer_Ver_3.2.2.exe) |
| ⚙️ C/C++ Library (DLL) | [⬇️ Download](./../../files/SLCAN_DLL_win.zip) |
| 📖 How to Add DLL in Your Project | [⬇️ Guide](https://learn.microsoft.com/en-us/cpp/build/walkthrough-creating-and-using-a-dynamic-link-library-cpp?view=msvc-170) |
| 🐍 Python Library (PyPI) | [`pip install dynosure-slcanv1`](https://pypi.org/project/dynosure-slcanv1/) |
| 📖 Python Library Documentation | [⬇️ Download](./../../files/slcanv1_documentation.pdf) |

---

# Trusted By

- Bgauss Pvt Ltd.
- RTCON Engineering
- Jindal Mobilitric Pvt Ltd.
- MATEL Motion and Energy Solutions Pvt Ltd.
- TRONTEK ELECTRONICS LIMITED
- Lord's Automative Private Limited

---

### 📞 **Ready to order?** Contact us at **+91 9898204057 (Mukesh Patel)** or **+91 9422556559**, or email **dynosure.india@gmail.com**
