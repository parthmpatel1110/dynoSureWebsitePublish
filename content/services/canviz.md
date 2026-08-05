---
title: "DynoSure CANviz — Browser-Based CAN Bus Analyzer"
date: 2018-11-18T12:33:46+10:00
draft: false
featured: true
weight: 5
image: "images/canviz_demo_trimmed.gif"
---

DynoSure CANviz is a browser-based, open-source CAN bus analyzer software. Based on the `canviz` project developed by **Chanchaldhiman**, it lets you plug in your USB-to-CAN adapter, run one command, and analyze CAN traffic instantly in your browser. No complex GUI installs, no accounts, and no internet connection required.

<!--more-->

![DynoSure CANviz Screenshot](/images/canviz_demo_trimmed.gif)

---

# 🚀 Quick Start

To install and run DynoSure CANviz on your local machine:

```bash
# Install the library from PyPI
python -m pip install dynosure-canviz

# Run the analyzer
python -m canviz
# Your browser will open automatically at http://localhost:8080
```

---

# 💡 Key Features

### 📊 Live Message Table
Monitor every frame on the CAN bus in real time. Features virtual scrolling to handle thousands of rows without dropouts or frame loss (tested up to 2,000 frames per second sustained).

### 🏷️ DBC Signal Decoding
Upload a standard `.dbc` file to translate raw hex payloads into named signal values shown directly inline. Toggle between raw and decoded views instantly.

### 📈 Signal Time-Series Plotting
Plot up to 8 decoded signals simultaneously on a shared time axis. Supports zooming, LTTB downsampling to save memory, per-signal threshold lines with breach alerts, and one-click PNG export.

### ⏱️ Multi-Frame Transmit
Build and maintain a custom transmit list, each frame with its own independent transmission timer/frequency (e.g., heartbeat at 20 Hz, speed signal at 10 Hz).

### ⏺️ Record & Replay
Record live CAN bus traffic directly into industry-standard `.asc` (Vector ASC) or `.csv` files. Replay log files at adjustable speeds from 0.5x up to 10x.

### 📉 Bus Health Statistics
Stay informed with always-visible real-time metrics, including received/transmitted frames, error frame counts, bus load percentage, and throughput.

---

# 🔌 Protocol Decoders

CANviz includes passive decoders that extract protocol events directly from bus traffic with no additional polling or configuration:

- **J1939 Decoder**: Auto-detection at 250 kbps, CAN ID decomposition (PGN, Priority, Source Address), built-in PGN/SA name dictionaries, BAM transport reassembly, and active fault (DM1) decoding.
- **CANopen Decoder (CiA 301 & 402)**: Auto-detection of node states, NMT command transmission, expedited SDO read/write, EMCY fault code decoding, and CiA 402 drive status/quick buttons for motor testing.

---

# 🔧 Hardware Compatibility

DynoSure CANviz works seamlessly with any standard USB-to-CAN hardware adapter, including:
* **DynoSure SLCANv1** and **SLCAN GPIO** adapters (via COM port/slcan interface configuration)
* Adapters running **Candlelight firmware** (plug-and-play gs_usb / WinUSB)
* PEAK PCAN-USB (via PEAK drivers)
* SocketCAN (Linux / Raspberry Pi / WSL2)

---

# 📄 Open Source & PyPI Links

- **GitHub Repository**: [DynoSure-TestedandVerified/DynoSure_CANviz](https://github.com/DynoSure-TestedandVerified/DynoSure_CANviz)
- **PyPI Package**: [dynosure-canviz on PyPI](https://pypi.org/project/dynosure-canviz/)
- **Based on**: [canviz by Chanchaldhiman](https://github.com/Chanchaldhiman/CANviz)

---

### 📞 **Need assistance?** Contact us at **+91 9898204057 (Mukesh Patel)** or **+91 9422556559 (Parth Patel)**, or email **dynosure.india@gmail.com**
