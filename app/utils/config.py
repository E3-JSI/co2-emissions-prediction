import os

def _int_env(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default

EXPORTER_NAME = 'kepler'
EXPORTER_URL = os.getenv('EXPORTER_URL', 'http://localhost:9102/metrics')
MODE = os.getenv("MODE", "local").strip().lower()
SAMPLE_INTERVAL = _int_env("SAMPLE_INTERVAL", _int_env("SCRAPE_INTERVAL", 10))
DB_FLUSH_INTERVAL = _int_env("DB_FLUSH_INTERVAL", 5)
TSDB_DSN = os.getenv("TSDB_DSN")
CHROME_BIN = os.getenv("CHROME_BIN")
CHROMEDRIVER_BIN = os.getenv("CHROMEDRIVER_BIN")

DEFAULT_CO2_INTENSITIES = {
    "DE": 148.0, "FR": 20.0, "IT": 160.0, "ES": 79.0, "GB": 136.0,
    "PL": 510.0, "NL": 83.0, "BE": 74.0, "AT": 26.0, "SE": 17.0,
    "SI": 46.0, "DK": 105.0, "NO": 31.0, "FI": 40.0, "CH": 26.0,
    "CZ": 339.0, "HU": 111.0, "RO": 309.0, "PT": 120.0, "IE": 329.0,
    "GR": 274.0, "HR": 183.0, "SK": 156.0, "BG": 311.0, "EE": 50.0,
    "LT": 95.0, "LV": 122.0, "IS": 28.0,
    "AL": 25.0, "AM": 190.0, "BY": 350.0, "BA": 620.0,
    "CY": 600.0, "GE": 150.0, "KZ": 650.0, "XK": 700.0, "LU": 65.0,
    "MT": 400.0, "MD": 380.0,  "ME": 450.0, "MK": 550.0,
    "TR": 420.0
}
