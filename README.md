# M.A.S.S. Device (Meteorological Analytics Streaming System)

The M.A.S.S. Device is a real-time streaming pipeline that ingests live weather data, applies rolling analytics (EWMA, z-scores, CUSUM), and visualizes anomalies to deliver timely insights and alerts on rapidly changing atmospheric conditions.

**Short tagline:** Real-time weather analytics & anomaly detection—live.

## Features
- Python producer polls a free weather API and publishes normalized JSON to Kafka.
- Python consumer maintains rolling windows, detects anomalies, and animates charts.
- Optional email/SMS alerts (pressure drops, wind gust thresholds, rapid change).
- Clear env configuration; simple, reproducible repo structure.

## Project Structure

mass_device/
|  - producers/
|  - consumers/
|  - utils/
|  - data/
|  - logs/
|  - README.md
|  - requirements.txt
|  - .env.example
|  - .gitignore
|  - LICENSE

## Quickstart

# 1) Create & Activate venv

# Windows
```shell
# If you have multiple Pythons installed, prefer py -3.11:
py -3.11 -m venv .venv
# Fallback (if 'py' not available and 'python' already points to 3.11):
# python -m venv .venv
. .\.venv\Scripts\Activate.ps1
```

# macOS/Linux
```bash
# Use the 3.11 interpreter explicitly if available:
python3.11 -m venv .venv
# Fallback if python3.11 not on PATH but python3 is 3.11:
# python3 -m venv .venv
source .venv/bin/activate
```

# Check Version
```bash
python -V   # should report Python 3.11.x inside the venv
```


# 2) Install Dependencies

# Windows
```shell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

# macOS/Linux
```bash
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```


# 3) Configure environment

# Windows
```shell
# PowerShell supports cp, but Copy-Item is explicit:
Copy-Item .env.example .env
# Edit .env with your keys, location, and Kafka bootstrap servers.
```

# macOS/Linux
```bash
cp .env.example .env
# Edit .env with your keys, location, and Kafka bootstrap servers.
```


# 4) Run Producer (Terminal One (1))

# Windows/macOS/Linux
```bash
python -m producers.weather_producer
```


# 5) Run Consumer (Terminal Two (2))

# Windows/macOS/Linux
```bash
python -m consumers.weather_consumer
```

