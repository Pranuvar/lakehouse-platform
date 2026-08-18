"""
Shared config/helpers for every seeder script. Kept dependency-light and
framework-free on purpose -- these scripts run once (or on demand) from a
host venv or `docker compose run`, not as part of the orchestrated platform,
so they don't need to share code with the Airflow/Spark layers.

Domain: "Fjord Mart" -- a fictitious Dublin-headquartered omnichannel
grocery-and-general-merchandise retailer trading online and through
physical stores across Ireland, the UK, and a handful of EU markets. Every
seeder in this directory generates one piece of Fjord Mart's data estate:

    seed_postgres_oltp.py -> the OLTP system of record (orders, customers...)
    seed_flatfiles.py     -> POS inventory snapshots dropped by store tills
    seed_kafka_events.py  -> website/app clickstream
    (mock-api service)    -> ad-platform campaign performance (3rd party)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Path = REPO_ROOT / ".env") -> None:
    """Minimal .env loader -- avoids adding python-dotenv as a dependency
    for something this small. Only sets vars not already in the environment,
    so real env vars (CI, shell exports) always win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv()

# --------------------------------------------------------------------- #
# Connection config -- these scripts run from the HOST (venv or your IDE),
# not inside the compose network, so every default below is the
# *published* port on localhost (see docker-compose.yml port mappings).
# Set SEEDER_* env vars to override, e.g. when running a seeder from
# inside a container attached to the compose network instead.
# --------------------------------------------------------------------- #
PG_HOST = os.environ.get("SEEDER_PG_HOST", "localhost")
PG_PORT = int(os.environ.get("SEEDER_PG_PORT", "5432"))
PG_USER = os.environ.get("POSTGRES_USER", "lakehouse")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "lakehouse_dev_pw")
PG_OLTP_DB = os.environ.get("POSTGRES_OLTP_DB", "oltp")

MINIO_ENDPOINT = os.environ.get("SEEDER_MINIO_ENDPOINT", "localhost:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse_dev_pw")
MINIO_BUCKET_DROPZONE = os.environ.get("MINIO_BUCKET_DROPZONE", "raw-dropzone")
MINIO_BUCKET_LAKEHOUSE = os.environ.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")

REDPANDA_BROKER = os.environ.get("SEEDER_REDPANDA_BROKER", "localhost:19092")
REDPANDA_TOPIC_EVENTS = os.environ.get("REDPANDA_TOPIC_EVENTS", "web.clickstream.events")

MOCK_API_BASE_URL = os.environ.get("SEEDER_MOCK_API_BASE_URL", "http://localhost:8000")
MOCK_API_KEY = os.environ.get("MOCK_API_KEY", "dev-local-key-do-not-use-in-prod")

# --------------------------------------------------------------------- #
# Shared domain reference data
# --------------------------------------------------------------------- #
SEED = 20260817  # deterministic across reruns: today's date as an int

TODAY = date.today()
HISTORY_DAYS = 730  # ~2 years of trading history, matches the mock API's window
HISTORY_START = TODAY - timedelta(days=HISTORY_DAYS - 1)

COUNTRIES = ["IE", "GB", "DE", "NL", "FR", "ES"]
COUNTRY_WEIGHTS = [0.42, 0.28, 0.10, 0.08, 0.07, 0.05]  # Ireland-headquartered, IE/GB heavy

CITIES_BY_COUNTRY = {
    "IE": ["Dublin", "Cork", "Galway", "Limerick", "Waterford", "Kilkenny"],
    "GB": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol"],
    "DE": ["Berlin", "Munich", "Hamburg", "Cologne"],
    "NL": ["Amsterdam", "Rotterdam", "Utrecht"],
    "FR": ["Paris", "Lyon", "Marseille"],
    "ES": ["Madrid", "Barcelona", "Valencia"],
}

PRODUCT_CATEGORIES = {
    "Grocery": ["Bakery", "Dairy & Eggs", "Fresh Produce", "Frozen Foods", "Pantry Staples", "Beverages"],
    "Household": ["Cleaning", "Laundry", "Paper Goods"],
    "Health & Beauty": ["Skincare", "Haircare", "Vitamins", "Oral Care"],
    "Home & Garden": ["Kitchenware", "Storage", "Garden Tools"],
    "Electronics": ["Small Appliances", "Accessories", "Batteries & Power"],
    "Baby & Kids": ["Nappies & Wipes", "Baby Food", "Toys"],
}

BRANDS = [
    "Fjord Basics", "Northgate", "Clonmel Kitchen", "Everline", "PureLeaf",
    "Anders & Co", "Waverton", "Solstice", "GreenAcre", "Kelvin & Wren",
    "Marlow Home", "Otterly", "Bramble Foods", "Northstar", "Halcyon",
]

PAYMENT_METHODS = ["card", "paypal", "apple_pay", "gift_card", "cash"]
ORDER_STATUSES = ["completed", "completed", "completed", "completed", "shipped", "cancelled", "refunded"]


def progress(label: str, done: int, total: int, start_ts: float, every: int = 500_000) -> None:
    if done % every != 0 and done != total:
        return
    elapsed = time.time() - start_ts
    rate = done / elapsed if elapsed > 0 else 0
    pct = 100.0 * done / total if total else 100.0
    sys.stdout.write(f"\r  {label}: {done:>10,}/{total:,} ({pct:5.1f}%)  {rate:,.0f} rows/s ")
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")
