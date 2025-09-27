# M.A.S.S. Device (Meteorological Analytics Streaming System)

The M.A.S.S. Device is a real-time streaming pipeline that ingests live weather data, applies rolling analytics (EWMA, z-scores, CUSUM), and visualizes anomalies to deliver timely insights and alerts on rapidly changing atmospheric conditions.


**Short tagline:** Real-time weather analytics & anomaly detection—live.


## Introduction


### Features
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


## Requirements

- **Python:** 3.11 (enforced via `pyproject.toml` or `python_requires`)
  - Verify inside venv: `python -V` → `Python 3.11.x`
- **Operating System:** Windows 11 (22H2/24H2) or macOS 12+ / Linux (x86_64)
- **Kafka:** Local or remote broker (e.g., Apache Kafka 3.x) reachable at `KAFKA_BOOTSTRAP_SERVERS`
  - Default assumption: `localhost:9092`
  - Topic: `weather_live` (created automatically by broker if auto-create is enabled; otherwise create manually)
- **Network/Ports:** Ensure the client can reach the Kafka broker port (default 9092)
- **Python packages:** Listed in `requirements.txt` (install inside the venv)
- **Environment variables:** Configure via `.env` (copy from `.env.example`)
  - `WEATHER_API_BASE` (e.g., Open-Meteo), `LOCATION_LAT`, `LOCATION_LON`, `POLL_SECONDS`
  - `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID`
  - Optional alerts: `ALERTS_ENABLED`, `ALERT_PRESSURE_DROP_HPA`, `ALERT_WIND_GUST_MPS`, `ALERT_COOLDOWN_MIN`
  - Optional email/SMS: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `ALERT_EMAIL_TO`, `ALERT_EMAIL_FROM`, and/or SMS gateway/API keys


### Recommended tooling

- **Windows PowerShell** (for commands in this README)
- **VS Code** with extensions:
  - Python, Pylance
  - Ruff or Black (formatting/linting)
  - dotenv (syntax highlighting for `.env`)
- **Git** (2.39+) for version control


### Optional enforcement of Python 3.11
Add one of the following to the repo to make the Python version explicit:

**Option A – `pyproject.toml`**
```bash
[project]
name = "mass_device"
version = "0.1.0"
requires-python = ">=3.11,<3.12"
```


**Option B – `setup.cfg`**
```bash
[metadata]
name = mass_device
version = 0.1.0

[options]
python_requires = >=3.11,<3.12
```


**Option C - `.python-version`**
```bash
3.11.9
```


**Quick Verification**
```bash
# Verify Python version in the venv
python -V

# Verify key environment variables are present
python - <<'PY'
import os
vars = [
  "WEATHER_API_BASE","LOCATION_LAT","LOCATION_LON","POLL_SECONDS",
  "KAFKA_BOOTSTRAP_SERVERS","KAFKA_TOPIC","KAFKA_CLIENT_ID","KAFKA_GROUP_ID"
]
missing = [v for v in vars if not os.getenv(v)]
print("Missing:", missing if missing else "None")
PY
```


## Tasks
1. Clone / open the project in VS Code.
2. Create & activate a Python 3.11 virtual environment.
3. Install dependencies from requirements.txt.
4. Create .env and set API_KEY=... (keep it off GitHub).
5. Run the consumer and verify the live chart updates.
6. Commit & push your changes, include a screenshot for submission.

## Quickstart

# 1) Create & Activate venv

# Windows
```shell
# If you have multiple Pythons installed, prefer py -3.11:
py -3.11 -m venv .venv
# Fallback (if 'py' not available and 'python' already points to 3.11):
# python -m venv .venv
.\.venv\Scripts\Activate.ps1
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


## API Key Hygiene (don’t commit secrets)
- Store the key in .env (and/or secrets/), both are git-ignored.
- Never hard-code your key in .py files.
- If you rotate keys, just update .env.


## Troubleshooting

## Authors

Contributors names and contact info <br>
@github.com/Data-Git-Hub <br>

---


## Version History
- P6 Init 0.2 | Modify README.md
- P6 Init     | Add requirements.txt, pyproject.toml, setup.cfg; Modify .gitignore, README.md


## Test History
