#!/usr/bin/env python3
import threading
import time
import logging
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import json

from flask import Flask, request, jsonify, Response
import requests
from prometheus_client.parser import text_string_to_metric_families

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# Configuration
EXPORTER_NAME = 'kepler'
EXPORTER_URL = os.getenv('EXPORTER_URL', 'http://localhost:9102/metrics')
MODE = os.getenv("MODE", "local")
SAMPLE_INTERVAL = 10 # seconds
# BLOCK_DURATION = 10 * 60 # 10 minutes in seconds
TSDB_DSN = os.getenv("TSDB_DSN")
# MODE ="db" # FORCING DB MODE FOR TESTING
# TSDB_DSN=None # FORCING MOCK DB FOR TESTING


# Data structures
@dataclass
class PowerMeasurement:
    timestamp: datetime
    joules_per_second: float
    namespace: str
    joules_total: float

@dataclass
class DataBlock:
    start_time: datetime
    end_time: datetime
    measurements: List[PowerMeasurement]
    measurement_limit: int
    is_complete: bool = False
    sent_to_db: bool = False

class TimeSeriesManager:
    """Manages time series data with local cache and DB fallback"""
    
    def __init__(self):
        self.blocks: Dict[Tuple[str, str, str], deque] = defaultdict(lambda: deque(maxlen=5))  # Keep last 5 blocks per (pod, container, namespace)
        self.current_block: Dict[Tuple[str, str, str], DataBlock] = {}
        self.last_joules: Dict[Tuple[str, str, str], float] = {}
        self.lock = threading.Lock()
        self.last_scrape_time: Optional[datetime] = None
        
    def add_measurement(self, pod: str, container: str, namespace: str, joules: float, timestamp: datetime):
        """Add a power measurement to the current block"""
        key = (pod, container, namespace)
        prev_joules = self.last_joules.get(key, joules)
        
        if key not in self.last_joules:
            # Initialize last_joules for a new key
            self.last_joules[key] = joules
            logging.info(f"Initialized last_joules for {key} with {joules}")
            # Start a new block immediately for this new key
            self._start_new_block(key, timestamp)
            
            return
        
        with self.lock:
            # Ensure a current block exists for this key and is not complete
            if key not in self.current_block or self.current_block[key].is_complete:
                self._start_new_block(key, timestamp)

            block = self.current_block[key]
            
            # Only add measurement if the block is not full
            if len(block.measurements) < block.measurement_limit:
                block.measurements.append(
                    PowerMeasurement(
                        timestamp=timestamp,
                        joules_per_second=(joules - prev_joules) / SAMPLE_INTERVAL,
                        namespace=namespace,
                        joules_total=joules
                    )
                )
                logging.debug(f"[TimeSeriesManager] Added measurement to block for {key}. Current measurements: {len(block.measurements)}")
                block.end_time = timestamp # Update end_time with the latest measurement
            
            # Check if block is now complete
            if len(block.measurements) >= block.measurement_limit:
                block.is_complete = True
                logging.info(f"[TimeSeriesManager] Block for {key} is complete with {len(block.measurements)} measurements.")
                self._finalize_block(key)
            
            self.last_joules[key] = joules
        # Removed debug log for unchanged joules, as it's less relevant with size-based blocks
    
    def _start_new_block(self, key: Tuple[str, str, str], timestamp: datetime, measurement_limit: int = 5):
        """Start a new data block with a defined measurement limit"""
        logging.debug(f"[TimeSeriesManager] Starting new block for {key} with measurement limit {measurement_limit}")
        self.current_block[key] = DataBlock(
            start_time=timestamp,
            end_time=timestamp + timedelta(seconds=SAMPLE_INTERVAL), # Initially set end_time to the next expected scrape time
            measurements=[],
            measurement_limit=measurement_limit,
            is_complete=False,
            sent_to_db=False
        )
    
    def _finalize_block(self, key: Tuple[str, str, str]):
        """Finalize a completed block and queue for DB storage"""
        print(f"[TimeSeriesManager] Finalizing block for {key}")
        block = self.current_block[key]
        if block.measurements:
            self.blocks[key].append(block)
            logging.debug(f"[TimeSeriesManager] Block finalized and added to blocks for {key}. Total blocks: {len(self.blocks[key])}")
            # Queue for DB storage ONLY if in DB mode.
            if MODE == "db" and db_manager:
                db_manager.queue_block_for_storage(key, block)
                print(f"[TimeSeriesManager] Block for {key} SHIPPED TO DB.") # Added print statement

class CO2IntensityManager:
    """Manages CO2 intensity data with time-based lookup"""
    
    _DEFAULT_INTENSITIES = {
        "DE": 148.0, "FR": 20.0, "IT": 160.0, "ES": 79.0, "GB": 136.0,
        "PL": 510.0, "NL": 83.0, "BE": 74.0, "AT": 26.0, "SE": 17.0,
        "SI": 46.0, "DK": 105.0, "NO": 31.0, "FI": 40.0, "CH": 26.0,
        "CZ": 339.0, "HU": 111.0, "RO": 309.0, "PT": 120.0, "IE": 329.0,
        "GR": 274.0, "HR": 183.0, "SK": 156.0, "BG": 311.0, "EE": 50.0,
        "LT": 95.0, "LV": 122.0, "IS": 28.0
    }

    def __init__(self):
        # self.intensities: Dict[str, Dict[datetime, float]] = defaultdict(dict) # Removed local cache
        self.lock = threading.Lock()

    def get_co2_data_for_country(self, country):
        retries = 0
        while retries < 3:
           # print(f"Fetching CO2 data for {country} (attempt {retries+1})")
            try:
                global driver
                if driver is None:
                    driver = create_driver()
                url = f"https://app.electricitymaps.com/zone/{country}/72h/hourly"
                driver.get(url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//p[@data-testid='co2-square-value']"))
                )
                soup = BeautifulSoup(driver.page_source, "html.parser")
                span = soup.find("p", {"data-testid": "co2-square-value"}).find("span", "font-semibold")
                if span:
                    #print(float(span.text.strip()))
                    return float(span.text.strip())
                raise RuntimeError("no CO2 span")
            except Exception as e:
                logging.warning(f"{country} attempt {retries+1} failed: {e}")
                retries += 1
                if driver:
                    driver.quit(); driver = None
                time.sleep(5)
        logging.error(f"CO2 fetch {country} failed")
        
        return None

    def get_intensity(self, country: str, timestamp: datetime) -> Optional[float]:
        """Get CO2 intensity for a country at a specific time, with fallback to default."""
        with self.lock:
            # Try to get from DB first
            if db_manager and MODE == "db":
                db_intensity = db_manager.get_co2_intensity_from_db(country, timestamp)
                if db_intensity is not None:
                    return db_intensity
            
            # Fallback to default intensity if no DB data or not in DB mode
            return self._DEFAULT_INTENSITIES.get(country)

    def update_intensity(self, country: str, value: float, timestamp: datetime = None):
        """Update CO2 intensity for a country at a specific time (stores in DB if in DB mode)."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        
        with self.lock:
            if db_manager and MODE == "db":
                db_manager.insert_co2_intensity(country, timestamp, value)
            # No local cache update needed
            logging.debug(f"CO2 intensity updated/stored for {country} at {timestamp.isoformat()}: {value}")

class DatabaseManager:
    """Manages database operations with batching and efficient queries"""
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None
        self.pending_blocks: List[Tuple[Tuple[str, str, str], DataBlock]] = []
        self.batch_size = 50  # Process 50 blocks at a time
        self.lock = threading.Lock()
        
    def connect(self):
        """Connect to database"""
        try:
            import psycopg2
            self.connection = psycopg2.connect(self.dsn)
            self.connection.autocommit = True
            logging.info("Connected to TimescaleDB")
        except Exception as e:
            logging.error(f"Failed to connect to TimescaleDB: {e}")
    
    def queue_block_for_storage(self, key: Tuple[str, str, str], block: DataBlock):
        """Queue a block for database storage"""
        with self.lock:
            self.pending_blocks.append((key, block))
            # Try to flush immediately; it is a no-op if not connected
            self._process_pending_blocks()
    
    def _process_pending_blocks(self):
        """Process pending blocks and store in database"""
        if not self.connection or not self.pending_blocks:
            return
        
        try:
            import psycopg2
            with self.connection.cursor() as cur:
                for (pod, container, ns), block in self.pending_blocks:
                    for measurement in block.measurements:
                        # Store raw measurement data
                        cur.execute("""
                            INSERT INTO container_metrics (
                                time, pod_id, namespace, energy_consumption, 
                                container_name, joules_total
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            measurement.timestamp, pod, ns,
                            measurement.joules_per_second, container, measurement.joules_total
                        ))
            stored = len(self.pending_blocks)
            self.pending_blocks = []
            logging.info(f"Stored {stored} blocks to database")
        except Exception as e:
            logging.error(f"Database write error: {e}")

    def insert_co2_intensity(self, country: str, timestamp: datetime, intensity: float):
        """Insert CO2 intensity data into the database."""
        if not self.connection:
            return
        try:
            import psycopg2
            with self.connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO co2_intensities (time, country_iso2, intensity_g_per_kwh)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (time, country_iso2) DO UPDATE SET intensity_g_per_kwh = EXCLUDED.intensity_g_per_kwh
                """, (timestamp, country, intensity))
            logging.debug(f"Inserted CO2 intensity for {country} at {timestamp.isoformat()}")
        except Exception as e:
            logging.error(f"Failed to insert CO2 intensity for {country} at {timestamp.isoformat()}: {e}")

    def get_co2_intensity_from_db(self, country: str, timestamp: datetime) -> Optional[float]:
        """Retrieve the most recent CO2 intensity for a given country at or before a specific timestamp."""
        if not self.connection:
            return None
        try:
            import psycopg2
            with self.connection.cursor() as cur:
                cur.execute("""
                    SELECT intensity_g_per_kwh
                    FROM co2_intensities
                    WHERE country_iso2 = %s AND time <= %s
                    ORDER BY time DESC
                    LIMIT 1
                """, (country, timestamp))
                result = cur.fetchone()
                if result:
                    return result[0]
                return None
        except Exception as e:
            logging.error(f"Failed to retrieve CO2 intensity for {country} at {timestamp.isoformat()}: {e}")
            return None

    def query_time_range(self, pod: str, container: str, namespace: str, start_time: datetime, end_time: datetime) -> List[dict]:
        """Query database for raw measurement data within a time range."""
        if not self.connection:
            return []
        
        try:
            import psycopg2
            with self.connection.cursor() as cur:
                # Query raw energy consumption data
                cur.execute("""
                    SELECT time, energy_consumption, namespace, pod_id, container_name, joules_total
                    FROM container_metrics 
                    WHERE pod_id = %s AND container_name = %s AND namespace = %s 
                    AND time >= %s AND time <= %s 
                    ORDER BY time
                """, (pod, container, namespace, start_time, end_time))
                
                results = []
                for row in cur.fetchall():
                    results.append({
                        "timestamp": row[0],
                        "joules_per_second": row[1],
                        "namespace": row[2],
                        "pod": row[3],
                        "container": row[4],
                        "joules_total": row[5]
                    })
                return results
        except Exception as e:
            logging.error(f"Database query error: {e}")
            return []

class MockDatabaseManager(DatabaseManager):
    """A mock database manager for testing and local development without a real DB."""
    def __init__(self):
        super().__init__(dsn="mock_dsn") # dsn is not used in mock
        self.connection = True  # Simulate a successful connection
        self.mock_db_data = defaultdict(list) # Stores raw container metrics data in memory
        self.mock_co2_intensities = defaultdict(dict) # Stores CO2 intensities in memory (country -> {timestamp -> intensity})

    def connect(self):
        logging.info("MockDatabaseManager connected.")
        print("[MockDatabaseManager] CONNECTED to mock DB.") # Added print statement

    def insert_co2_intensity(self, country: str, timestamp: datetime, intensity: float):
        """Mock insert CO2 intensity data."""
        with self.lock:
            self.mock_co2_intensities[country][timestamp] = intensity
            logging.debug(f"Mock inserted CO2 intensity for {country} at {timestamp.isoformat()}")

    def get_co2_intensity_from_db(self, country: str, timestamp: datetime) -> Optional[float]:
        """Mock retrieve the most recent CO2 intensity."""
        with self.lock:
            country_data = self.mock_co2_intensities.get(country, {})
            if country_data:
                available_times = sorted(country_data.keys(), reverse=True)
                for time_key in available_times:
                    if time_key <= timestamp:
                        return country_data[time_key]
            return None
    
    def _process_pending_blocks(self):
        """Process pending blocks and store in mock database"""
        if not self.connection or not self.pending_blocks:
            return

        stored = 0
        for (pod, container, ns), block in self.pending_blocks:
            for measurement in block.measurements:
                # In mock mode, we're storing the raw measurements directly
                # We'll calculate CO2 on retrieval, just like the real DB would for raw data.
                self.mock_db_data[(pod, container, ns)].append({
                    "time": measurement.timestamp,
                    "pod_id": pod,
                    "namespace": ns,
                    "energy_consumption": measurement.joules_per_second,
                    "container_name": container,
                    "joules_total": measurement.joules_total
                })
            stored += 1
        self.pending_blocks = []
        logging.info(f"Stored {stored} blocks to mock database")

    def query_time_range(self, pod: str, container: str, namespace: str, start_time: datetime, end_time: datetime) -> List[dict]:
        """Query mock database for raw measurement data within a time range."""
        key = (pod, container, namespace)
        results = []
        for record in self.mock_db_data.get(key, []):
            record_timestamp = record["time"]
            if start_time <= record_timestamp <= end_time:
                results.append({
                    "timestamp": record_timestamp,
                    "joules_per_second": record["energy_consumption"],
                    "namespace": record["namespace"],
                    "pod": record["pod_id"],
                    "container": record["container_name"],
                    "joules_total": record["joules_total"]
                })
        return results

# Global instances
time_series_manager = TimeSeriesManager()
co2_manager = CO2IntensityManager()
db_manager: Optional[DatabaseManager] = None

if MODE == "db":
    if TSDB_DSN:
        db_manager = DatabaseManager(TSDB_DSN)
        db_manager.connect()
        logging.info("Running in full DB mode.")
    else:
        logging.info("Running in mock DB mode (MODE=db but no TSDB_DSN). Blocks will be stored in memory only.")
        db_manager = MockDatabaseManager()
        db_manager.connect()
else:
    logging.info("Running in local mode (no database retention). Blocks are retained in memory cache only.")
    db_manager = None # Explicitly no DB manager

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# Selenium driver setup
driver = None
def create_driver():
    global driver
    if driver:
        driver.quit()
    
    chrome_options = Options()
    # Use the installed Chromium/Chrome in container; do not pin version
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.browser_version = "114"
    #print(f"DEBUG: create_driver called with options: {chrome_options.arguments}") # Added for debugging
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def scrape_kepler_once():
    """Scrape Kepler metrics once"""
    logging.debug("DEBUG: scrape_kepler_once is being called.") # Added for debugging
    try:
        headers = {'Cache-Control': 'no-cache'}
        resp = requests.get(EXPORTER_URL, timeout=5, headers=headers)
        resp.raise_for_status()
        #print(f"DEBUG: Raw Kepler response: {resp.text[:500]}...") # Temporarily print first 500 chars of response
        now = datetime.now(timezone.utc)
        changed = False

        for fam in text_string_to_metric_families(resp.text):
            #print(f"Processing metric family: {fam.name}")
            if fam.name != "kepler_container_joules":
                continue
            for sample in fam.samples:
                if sample.name != "kepler_container_joules_total":
                    continue
                #print(f"DEBUG: Processing sample: {sample}") # Added for debugging
                pod = sample.labels.get("pod_name")
                container = sample.labels.get("container_name")
                namespace = sample.labels.get("container_namespace")
                mode = sample.labels.get("mode")
                #print("DEBUG: Sample labels:", sample.labels) # Added for debugging
                logging.debug(f"  Sample labels: pod={pod}, container={container}, namespace={namespace}, mode={mode}")

                if mode != "dynamic" or not pod or not container:
                    logging.debug(f"  Skipping sample due to filter: mode={mode}, pod={pod}, container={container}")
                    continue
                    
                joules = sample.value
                prev_joules = time_series_manager.last_joules.get((pod, container, namespace))
                
                logging.info(f"  Busybox/Container ({pod}/{container}) joules_total - current: {joules}, previous: {prev_joules}")

                if prev_joules is None or joules != prev_joules:
                    changed = True
                    logging.info(f"  Adding measurement for {pod}/{container}: joules={joules}")
                    #print(f"DEBUG: Adding measurement for {pod}/{container} with joules={joules}") # Added for debugging
                    time_series_manager.add_measurement(pod, container, namespace, joules, now)
                else:
                    # No change in joules; add measurement anyway to maintain 
                    time_series_manager.add_measurement(pod, container, namespace, joules, now)
                    logging.debug(f"  Skipping measurement for {pod}/{container}: joules not changed.")

        time_series_manager.last_scrape_time = now
        return changed
    except Exception as e:
        logging.error(f"Kepler scrape failed: {e}")
        return False

def sample_kepler_loop():
    """Background loop to sample Kepler metrics"""
    logging.debug("DEBUG: sample_kepler_loop thread started.") # Added for debugging
    while True:
        t0 = datetime.now(timezone.utc)
        updated = scrape_kepler_once()
        if updated:
            logging.info("New Kepler data stored.")
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        time.sleep(max(0, SAMPLE_INTERVAL - elapsed))

def update_co2_intensities():
    """Update CO2 intensities aligned to the top of the hour using Selenium for all known zones on electricitymaps."""

    # Lightly scrape the list of available zones from the landing page once per process
    def get_all_zones() -> List[str]:
        try:
            global driver
            if driver is None:
                driver = create_driver()
            driver.get("https://app.electricitymaps.com/map")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            # Fallback: if no explicit list is present, keep previously known keys
            with co2_manager.lock:
                return list(co2_manager.intensities.keys()) or []
        except Exception:
            with co2_manager.lock:
                return list(co2_manager.intensities.keys()) or []

    def do_update():
        logging.info("Updating CO2 intensities…")
        zones = get_all_zones()
        # If we have no zones yet, seed with a small common set to bootstrap; will grow over time
        if not zones:
            zones = ["DE", "FR", "IT", "ES", "GB", "PL", "NL", "BE", "AT", "SE", "DK", "NO", "FI", "CH"]
        for zone in list(zones):
            v = co2_manager.get_co2_data_for_country(zone)
            if v is not None:
                hour_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
                co2_manager.update_intensity(zone, v, hour_dt)
        logging.info("CO2 update done")

    # Sleep until the start of the next hour
    now_dt = datetime.now(timezone.utc)
    next_hour_dt = (now_dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    seconds_until_next_hour = (next_hour_dt - now_dt).total_seconds()
    time.sleep(max(0, seconds_until_next_hour))
    while True:
        do_update()
        time.sleep(3600)

# Start background threads
threading.Thread(target=sample_kepler_loop, daemon=True).start()
threading.Thread(target=update_co2_intensities, daemon=True).start()



# Initial CO2 data loading
def preload_co2_data():
    """Preload CO2 data for available countries once."""
    seeds = ["DE", "FR", "IT", "ES", "GB", "PL", "NL", "BE", "AT", "SE", "DK", "NO", "FI", "CH"]
    for c in seeds:
        v = co2_manager.get_co2_data_for_country(c)
        if v is not None:
            hour_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            co2_manager.update_intensity(c, v, hour_dt)

def log_time_series_manager_state():
    """Logs the current state of the TimeSeriesManager for debugging."""
    while True:
        time.sleep(10) # Log every 10 seconds
        with time_series_manager.lock:
            print("\n--- TimeSeriesManager State (every 10s) ---")
            print(f"Last Scrape Time: {datetime.fromtimestamp(time_series_manager.last_scrape_time).isoformat() if time_series_manager.last_scrape_time else 'N/A'}")
            
            print("Current Blocks:")
            if not time_series_manager.current_block:
                print("  (No current blocks)")
            for key, block in time_series_manager.current_block.items():
                print(f"  Key: {key}, Start Time: {datetime.fromtimestamp(block.start_time).isoformat()}, Measurements: {len(block.measurements)}")

            print("Completed Blocks:")
            if not time_series_manager.blocks:
                print("  (No completed blocks)")
            for key, blocks_deque in time_series_manager.blocks.items():
                print(f"  Key: {key}, Total Completed Blocks: {len(blocks_deque)}")
            
            print("Last Joules:")
            if not time_series_manager.last_joules:
                print("  (No last joules records)")
            for key, joules in time_series_manager.last_joules.items():
                print(f"  Key: {key}, Last Joules: {joules}")
            print("-------------------------------------------")

#threading.Thread(target=log_time_series_manager_state, daemon=True).start()

@app.route('/api/containers', methods=['GET'])
def get_container_pod_pairs():
    """Get list of monitored (pod, container, namespace) tuples"""
    with time_series_manager.lock:
        keys = set(time_series_manager.blocks.keys()) | set(time_series_manager.current_block.keys())
        result = [{"pod": k[0], "container": k[1], "namespace": k[2]} for k in keys]
        return jsonify(result)

@app.route('/api/power-consumption', methods=['POST'])
def get_power():
    """Get recent power consumption data"""
    data = request.get_json() or {}
    n_blocks = int(data.get("n", 5))
    
    result = []
    with time_series_manager.lock:
        for (pod, container, ns), blocks in time_series_manager.blocks.items():
            print(f"DEBUG: Processing blocks for {pod}/{container} in namespace {ns}") # Added for debugging
            recent_blocks = list(blocks)[-n_blocks:]
            
            if recent_blocks:
                print(f"DEBUG: Found {len(recent_blocks)} recent blocks for {pod}/{container}") # Added for debugging
                result.append({
                    "pod": pod,
                    "container": container,
                    "namespace": ns,
                    "blocks": [
                        {
                            "start_time": block.start_time.isoformat(),
                            "end_time": block.end_time.isoformat(),
                            "measurement_count": len(block.measurements),
                            "measurements": [
                                {
                                    "timestamp": m.timestamp.isoformat(),
                                    "joules_per_second": m.joules_per_second,
                                    "namespace": m.namespace, # Assuming namespace is still relevant per measurement
                                    "joules_total": m.joules_total
                                }
                                for m in block.measurements
                            ],
                            "is_complete": block.is_complete,
                            "sent_to_db": block.sent_to_db
                        }
                        for block in recent_blocks
                    ]
                })
    
    return jsonify(result)

@app.route('/api/co2-per-container', methods=['POST'])
def co2_per_container():
    """Retrieves CO2 emissions data per container for specified time range or last N measurements."""
    data = request.get_json()
    
    # Filtering parameters
    target_pod = data.get('pod')
    target_container = data.get('container')
    target_namespace = data.get('namespace')
    target_countries = data.get('countries', []) # List of ISO2 country codes

    # Time or N measurements parameters
    start_time = data.get('start_time') # Unix timestamp
    end_time = data.get('end_time')     # Unix timestamp
    n_measurements = data.get('n')      # Last N individual measurements

    # Convert start_time and end_time to datetime objects if provided
    if start_time:
        start_time = datetime.fromisoformat(start_time)
    if end_time:
        end_time = datetime.fromisoformat(end_time)

    if not target_pod or not target_container or not target_namespace:
        return jsonify({"error": "pod, container, and namespace are required"}), 400

    if not target_countries:
        return jsonify({"error": "At least one country code is required"}), 400

    if (start_time is None or end_time is None) and n_measurements is None:
        return jsonify({"error": "Either (start_time and end_time) or n must be provided"}), 400

    if start_time is not None and end_time is not None and n_measurements is not None:
        return jsonify({"error": "Cannot provide both time range and n_measurements"}), 400

    response_measurements = []
    all_fetched_measurements = []
    
    with time_series_manager.lock:
        key = (target_pod, target_container, target_namespace)
        
        # 1. Retrieve data from local memory (time_series_manager)
        local_measurements_flat = []
        for block in time_series_manager.blocks.get(key, deque()):
            local_measurements_flat.extend(block.measurements)
        if time_series_manager.current_block.get(key):
            local_measurements_flat.extend(time_series_manager.current_block[key].measurements)
        
        all_fetched_measurements.extend(local_measurements_flat)

        # 2. If in DB mode, retrieve data from database
        if MODE == "db" and db_manager:
            db_measurements = []
            if start_time and end_time:
                db_measurements = db_manager.query_time_range(
                    pod=target_pod, container=target_container, namespace=target_namespace,
                    start_time=start_time, end_time=end_time
                )
            elif n_measurements is not None:
                # If n is provided, query a broad range from DB and then filter.
                # A more precise implementation would dynamically determine the required time window
                # based on the oldest local measurement, but for simplicity, we'll fetch a large window.
                # Assuming 24 hours of data is usually enough to cover N measurements.
                approx_start_time = datetime.now(timezone.utc) - timedelta(days=1) # Fetch last 24 hours
                db_measurements = db_manager.query_time_range(
                    pod=target_pod, container=target_container, namespace=target_namespace,
                    start_time=approx_start_time, end_time=datetime.now(timezone.utc)
                )
            
            # Convert DB results to PowerMeasurement objects for consistent processing
            for db_m in db_measurements:
                all_fetched_measurements.append(
                    PowerMeasurement(
                        timestamp=db_m["timestamp"], # Already datetime from query_time_range
                        joules_per_second=db_m["joules_per_second"],
                        namespace=db_m["namespace"],
                        joules_total=db_m.get("joules_total", 0.0) # joules_total might not be in older DB records
                    )
                )

    # 3. Deduplicate and sort all fetched measurements
    # Using a set of tuples (timestamp, joules_per_second) for unique identification
    unique_measurements = {}
    for m in all_fetched_measurements:
        # Using a tuple of relevant fields as a unique key for deduplication
        unique_key = (m.timestamp, m.joules_per_second, m.namespace)
        if unique_key not in unique_measurements:
            unique_measurements[unique_key] = m
    
    combined_and_sorted_measurements = sorted(unique_measurements.values(), key=lambda m: m.timestamp)

    # 4. Apply final filtering by time or N measurements
    final_filtered_measurements = []
    if n_measurements is not None:
        final_filtered_measurements = combined_and_sorted_measurements[-n_measurements:]
    elif start_time is not None and end_time is not None:
        for m in combined_and_sorted_measurements:
            if start_time <= m.timestamp <= end_time:
                final_filtered_measurements.append(m)

    # 5. Calculate CO2 emissions for each measurement and country
    for measurement in final_filtered_measurements:
        co2_data_for_measurement = {}
        for country in target_countries:
            intensity = co2_manager.get_intensity(country, measurement.timestamp)
            if intensity is not None:
                energy_joules = measurement.joules_per_second * SAMPLE_INTERVAL
                energy_kwh = energy_joules / (3.6 * 10**6)
                co2_emissions_g = energy_kwh * intensity
                co2_data_for_measurement[country] = {
                    "g_co2_emissions": co2_emissions_g,
                }
            else:
                co2_data_for_measurement[country] = {"error": "CO2 intensity not available for this time and country"}
        
        response_measurements.append({
            "timestamp": measurement.timestamp.isoformat(),
            "joules_per_second": measurement.joules_per_second,
            "co2_emissions_by_country": co2_data_for_measurement
        })

    return jsonify(response_measurements)

@app.route('/api/export-time-series', methods=['GET'])
def export_time_series():
    """Export all time series data as JSON"""
    data = []
    with time_series_manager.lock:
        for (pod, container, ns), blocks in time_series_manager.blocks.items():
            for block in blocks:
                for measurement in block.measurements:
                    data.append({
                        "pod": pod,
                        "container": container,
                        "namespace": ns,
                        "timestamp": measurement.timestamp.isoformat(),
                        "joules_per_second": measurement.joules_per_second,
                        "block_start": block.start_time.isoformat(),
                        "block_end": block.end_time.isoformat()
                    })
    return jsonify(data)

@app.route('/api/co2-intensities', methods=['GET'])
def get_co2_intensities():
    """Return the latest known CO2 intensity per tracked country (hourly) from the database."""
    payload = []
    
    # Get all countries for which we have default intensities (or scrape to get all)
    countries = list(CO2IntensityManager._DEFAULT_INTENSITIES.keys())
    current_time = datetime.now(timezone.utc)

    if db_manager and MODE == "db":
        with co2_manager.lock:
            for country in countries:
                intensity = db_manager.get_co2_intensity_from_db(country, current_time)
                if intensity is not None:
                    payload.append({
                        "country": country,
                        "hour": current_time.replace(minute=0, second=0, microsecond=0).isoformat(),
                        "value": intensity
                    })
    else:
        # In local/mock mode, we can't query the mock DB for the latest. 
        # We'll just return the defaults or what's in co2_manager.intensities (if it existed) 
        # but since we removed it, we'll return defaults for now.
        for country, intensity in CO2IntensityManager._DEFAULT_INTENSITIES.items():
            payload.append({
                "country": country,
                "hour": current_time.replace(minute=0, second=0, microsecond=0).isoformat(),
                "value": intensity
            })
    
    return jsonify(payload)

@app.route('/api/co2-range', methods=['POST'])
def co2_range():
    """Alias for /api/co2-per-container with explicit start/end time in request body."""
    return co2_per_container()

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    with time_series_manager.lock:
        total_blocks = sum(len(blocks) for blocks in time_series_manager.blocks.values())
        current_blocks = len(time_series_manager.current_block)
    
    return jsonify({
        "status": "healthy",
        "monitored_containers": len(time_series_manager.blocks),
        "total_blocks": total_blocks,
        "current_blocks": current_blocks,
        "mode": MODE,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route('/api/status', methods=['GET'])
def status():
    """Detailed status endpoint for debugging"""
    with time_series_manager.lock:
        unique_container_keys = len(time_series_manager.blocks) + len(time_series_manager.current_block)
        has_data = unique_container_keys > 0 or any(time_series_manager.blocks.values())

    last_scrape_time = time_series_manager.last_scrape_time
    last_measurement_time = None
    with time_series_manager.lock:
        if time_series_manager.current_block:
            # Get the maximum timestamp from all current blocks' measurements
            all_measurements = [
                m.timestamp for block in time_series_manager.current_block.values() for m in block.measurements
            ]
            if all_measurements:
                last_measurement_time = max(all_measurements)
        elif time_series_manager.blocks:
            # If no current blocks, get from the latest completed block
            all_block_measurements = [
                m.timestamp for blocks_deque in time_series_manager.blocks.values()
                for block in blocks_deque for m in block.measurements
            ]
            if all_block_measurements:
                last_measurement_time = max(all_block_measurements)

    pending_db_blocks = 0
    if db_manager and MODE == "db":
        with db_manager.lock:
            pending_db_blocks = len(db_manager.pending_blocks)

    return jsonify({
        "status": "operational",
        "last_scrape_time": last_scrape_time.isoformat() if last_scrape_time else "N/A",
        "last_measurement_time": last_measurement_time.isoformat() if last_measurement_time else "N/A",
        "has_data": has_data,
        "unique_container_keys": unique_container_keys,
        "pending_db_blocks": pending_db_blocks,
        "mode": MODE,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

if __name__ == '__main__':
    # Run potentially slow CO2 preload in background so the server can bind quickly
    threading.Thread(target=preload_co2_data, daemon=True).start()
    app.run(host='0.0.0.0', port=5001, debug=False)
