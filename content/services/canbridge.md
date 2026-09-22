---
title: "DynoSure CAN Bridge - Dual-Channel CAN Bus Bridge & Router"
date: 2018-11-18T12:33:46+10:00
draft: false
featured: true
weight: 6
image: "images/CanBridge.png"
---

The DynoSure CAN Bridge is an intelligent dual-channel CAN bus bridge and router powered by the Raspberry Pi RP2350 microcontroller. Featuring two independent CAN channels (CAN0 and CAN1) on a single rugged DB9 connector, it enables real-time message forwarding, filtering, ID translation, baud rate conversion, and bus isolation between two separate CAN networks without needing a PC.

<!--more-->

![DynoSure CAN Bridge](/images/CanBridge.png)

---

# 💡 Why Did We Build the DynoSure CAN Bridge?

In automotive and industrial systems, different electronic control units (ECUs) and subsystems frequently operate across isolated CAN networks or at differing bitrates (such as a 250 kbps vehicle powertrain bus and a 500 kbps telemetry network). Bridging, monitoring, or testing these networks typically requires expensive gateway hardware or complicated multi-adapter PC setups.

The **DynoSure CAN Bridge** solves this challenge by providing a compact, standalone bridge that connects two physical CAN buses directly. It handles real-time message forwarding, baud rate conversion, and message filtering completely autonomously.

---

# Key Applications

| Application | Description |
|---|---|
| **Baud Rate Conversion** | Seamlessly bridge buses operating at different bitrates (e.g., 250 kbps to 500 kbps). |
| **ID Filtering & Routing** | Selectively block or pass specific CAN IDs between networks to manage bus load. |
| **Bus & Subnet Isolation** | Protect critical ECU subnets from non-critical, high-load, or diagnostic traffic. |
| **Message Translation** | Forward, re-map, or filter message frames in real time between channels. |

---

# Specifications

| Parameter | Details |
|---|---|
| **Microcontroller** | Raspberry Pi RP2350 |
| **CAN Interfaces** | 2 Independent Channels (CAN0 & CAN1) |
| **Supported CAN Protocols** | CAN 2.0A (11-bit Standard ID), CAN 2.0B (29-bit Extended ID) |
| **Bitrates** | Configurable per channel (up to 1 Mbps) |
| **USB Interface** | USB 2.0 (for firmware flashing / UF2 bootloader) |
| **Power Supply & Operating Modes** | • **Bridge Mode**: Requires **+12V DC** external power supply via DB9 connector (**Pin 9: +12V**, **Pin 3: GND**)<br>• **USB Mode**: USB powered - acts as an **RP2350 USB mass storage drive** for firmware flashing |
| **Firmware Update** | Drag-and-drop `.uf2` file into **RP2350** drive |
| **Operating Temperature** | Extended range - suitable for industrial & automotive environments |

---

# 🔌 DB9 Pinout Mapping

Both independent CAN channels and power supply are integrated into a single standard DB9 connector:

![DB9 Connector Pinout](/images/db9_connector.png)

| DB9 Pin | Assignment | Description |
|---|---|---|
| **Pin 1** | **CAN1-L** | Channel 1 CAN Low |
| **Pin 2** | **CAN0-L** | Channel 0 CAN Low |
| **Pin 3** | **GND** | Power & Signal Ground |
| **Pin 7** | **CAN0-H** | Channel 0 CAN High |
| **Pin 8** | **CAN1-H** | Channel 1 CAN High |
| **Pin 9** | **+12V DC** | External Power Input (Bridge Mode) |
| **Others** | Not Connected | Reserved |

---

# ⚙️ Firmware Update Procedure

The DynoSure CAN Bridge utilizes the Raspberry Pi UF2 bootloader for effortless firmware updates:

1. Connect the CAN Bridge to your PC using a standard USB cable.
2. The device automatically enumerates as an **RP2350** mass storage drive.
3. Drag and drop (or copy) the `.uf2` firmware file into the drive.
4. The device flashes the firmware automatically and restarts ready for operation.

---

# Downloads

| Resource | Link |
|---|---|
| 📄 Product Datasheet | [⬇️ Download](./../../files/DynoSure_CANBridge_Datasheet.pdf) |

---

# Trusted By

- Bgauss Pvt Ltd.
- RTCON Engineering
- Jindal Mobilitric Pvt Ltd.
- MATEL Motion and Energy Solutions Pvt Ltd.
- TRONTEK ELECTRONICS LIMITED
- Lord's Automative Private Limited

---

### 📞 **Need Support or Ready to Order?**
- **Technical Support:** **+91 9422556559**
- **Sales & Inquiries:** **+91 9898204057 (Mukesh Patel)**
- **Email:** **dynosure.india@gmail.com**
