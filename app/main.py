import threading
import time
import logging
from datetime import datetime, timedelta, timezone
import requests
from flask import Flask, jsonify, request
from .services.managers import TimeSeriesManager, CO2IntensityManager
from .services.scraper import scrape_kepler_metrics, scrape_electricity_maps, get_selenium_driver
from .database import DatabaseManager
from .utils.config import MODE, TSDB_DSN, SAMPLE_INTERVAL, EXPORTER_URL, DB_FLUSH_INTERVAL

app = Flask(__name__)
co2_ready_event = threading.Event()
co2_manager = CO2IntensityManager(ready_event=co2_ready_event, max_workers=2)
background_thread = None
co2_thread = None
db_thread = None
health_lock = threading.Lock()
health_state = {
    "started_at": datetime.now(timezone.utc),
    "startup_check_completed": False,
    "startup_check_ok": False,
    "startup_errors": [],
    "last_kepler_scrape_success": None,
    "last_kepler_scrape_error": None,
}

if MODE == "db":
    if TSDB_DSN:
        db_manager = DatabaseManager(TSDB_DSN)
        db_manager.connect()
    else:
        db_manager = None
else:
    db_manager = None

def on_block_complete(key, block):
    if MODE == "db" and db_manager:
        db_manager.queue_block_for_storage(key, block)

ts_manager = TimeSeriesManager(on_block_complete=on_block_complete)

def run_startup_self_check():
    errors = []

    if SAMPLE_INTERVAL <= 0:
        errors.append("SAMPLE_INTERVAL must be a positive integer")

    try:
        resp = requests.get(EXPORTER_URL, timeout=3)
        resp.raise_for_status()
    except Exception as e:
        errors.append(f"Exporter check failed: {e}")

    try:
        driver = get_selenium_driver()
        driver.quit()
    except Exception as e:
        errors.append(f"Selenium check failed: {e}")

    if MODE not in {"local", "db"}:
        errors.append("MODE must be one of: local, db")

    if MODE == "db":
        if not TSDB_DSN:
            errors.append("TSDB_DSN is required in MODE=db")
        if not db_manager or not db_manager.connection:
            errors.append("DB connection check failed")

    with health_lock:
        health_state["startup_check_completed"] = True
        health_state["startup_check_ok"] = len(errors) == 0
        health_state["startup_errors"] = errors

    if errors:
        logging.error(f"Startup self-check failed: {errors}")
    else:
        logging.info("Startup self-check passed.")

def background_loop():
    while True:
        if not co2_ready_event.is_set():
            logging.info("KEPLER THREAD: Waiting for CO2 intensities to initialize...")
            co2_ready_event.wait() 
        
        scrape_ok = scrape_kepler_metrics(ts_manager)
        with health_lock:
            if scrape_ok:
                health_state["last_kepler_scrape_success"] = datetime.now(timezone.utc)
            else:
                health_state["last_kepler_scrape_error"] = datetime.now(timezone.utc)
        time.sleep(SAMPLE_INTERVAL)

def db_loop():
    if not db_manager:
        return
    while True:
        try:
            db_manager.process_pending_blocks()
        except Exception as e:
            logging.error(f"DB THREAD: Flush failed: {e}")
        time.sleep(DB_FLUSH_INTERVAL)

def co2_loop():
    """Immediately updates CO2 and then schedules for the top of every hour."""
    logging.info("CO2 THREAD: Starting...")
    co2_ready_event.set()
    
    try:
        logging.info("CO2 THREAD: Performing initial intensity scrape...")
        co2_manager.update_intensities(scrape_electricity_maps)
    except Exception as e:
        logging.error(f"CO2 THREAD: Initial scrape failed: {e}")

    while True:
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
        wait_seconds = (next_hour - now).total_seconds()
        
        logging.info(f"CO2 THREAD: Sleeping {wait_seconds:.0f}s until next hourly update ({next_hour}).")
        time.sleep(wait_seconds)
        
        try:
            logging.info("CO2 THREAD: Performing scheduled hourly scrape...")
            co2_manager.update_intensities(scrape_electricity_maps)
        except Exception as e:
            logging.error(f"CO2 THREAD: Hourly scrape failed: {e}")

@app.route('/api/containers')
def get_containers():
    with ts_manager.lock:
        keys = set(ts_manager.blocks.keys()) | set(ts_manager.current_block.keys())
    return jsonify([{"pod": k[0], "container": k[1], "namespace": k[2]} for k in keys])

@app.route('/api/co2-per-container', methods=['POST'])
def get_co2_per_container():
    data = request.json or {}
    pod, container, ns = data.get('pod'), data.get('container'), data.get('namespace')
    countries = data.get('countries', ['DE'])
    n = data.get('n', 5)
    start_time_raw = data.get('start_time')
    end_time_raw = data.get('end_time')

    if not pod or not container or not ns:
        return jsonify({"error": "pod, container, and namespace are required"}), 400

    if not isinstance(countries, list) or not countries or not all(isinstance(c, str) and c for c in countries):
        return jsonify({"error": "countries must be a non-empty list of country codes"}), 400

    if (start_time_raw and not end_time_raw) or (end_time_raw and not start_time_raw):
        return jsonify({"error": "Both start_time and end_time must be provided together"}), 400

    start_time = None
    end_time = None
    if start_time_raw and end_time_raw:
        try:
            start_time = datetime.fromisoformat(str(start_time_raw).replace("Z", "+00:00"))
            end_time = datetime.fromisoformat(str(end_time_raw).replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"error": "Invalid start_time/end_time format. Use ISO-8601"}), 400

        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        if start_time > end_time:
            return jsonify({"error": "start_time must be <= end_time"}), 400

    try:
        if start_time and end_time:
            selection_mode = "range"
            if MODE == "db":
                if not db_manager or not db_manager.connection:
                    return jsonify({"error": "Database unavailable in MODE=db"}), 503
                db_manager.process_pending_blocks()
                selected_measurements = db_manager.get_measurements(
                    pod, container, ns, start_time=start_time, end_time=end_time
                )
                with ts_manager.lock:
                    current_block = ts_manager.current_block.get((pod, container, ns))
                    if current_block:
                        selected_measurements.extend(
                            m for m in current_block.measurements if start_time <= m.timestamp <= end_time
                        )
            else:
                with ts_manager.lock:
                    blocks = list(ts_manager.blocks.get((pod, container, ns), []))
                    current_block = ts_manager.current_block.get((pod, container, ns))

                selected_measurements = []
                for block in blocks:
                    selected_measurements.extend(
                        m for m in block.measurements if start_time <= m.timestamp <= end_time
                    )
                if current_block:
                    selected_measurements.extend(
                        m for m in current_block.measurements if start_time <= m.timestamp <= end_time
                    )
        else:
            try:
                n = int(n)
                if n <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                return jsonify({"error": "n must be a positive integer"}), 400

            selection_mode = "last_n"
            if MODE == "db":
                if not db_manager or not db_manager.connection:
                    return jsonify({"error": "Database unavailable in MODE=db"}), 503
                db_manager.process_pending_blocks()
                selected_measurements = db_manager.get_measurements(
                    pod, container, ns, n=n
                )
                with ts_manager.lock:
                    current_block = ts_manager.current_block.get((pod, container, ns))
                    if current_block:
                        selected_measurements.extend(current_block.measurements)
            else:
                with ts_manager.lock:
                    blocks = list(ts_manager.blocks.get((pod, container, ns), []))
                    current_block = ts_manager.current_block.get((pod, container, ns))

                selected_measurements = []
                for block in blocks:
                    selected_measurements.extend(block.measurements)
                if current_block:
                    selected_measurements.extend(current_block.measurements)

            selected_measurements.sort(key=lambda m: m.timestamp)
            selected_measurements = selected_measurements[-n:]
    except Exception as e:
        logging.error(f"Measurement query failed for {(pod, container, ns)}: {e}")
        return jsonify({"error": "Failed to query measurements"}), 500

    if not selected_measurements:
        return jsonify({"error": "No data found for the selected window"}), 404

    selected_measurements.sort(key=lambda m: m.timestamp)

    results = {}

    for country in countries:
        total_co2 = 0
        total_joules = 0
        measurements_payload = []
        for m in selected_measurements:
            intensity = co2_manager.get_intensity(country, m.timestamp)
            energy_j = m.joules_per_second * SAMPLE_INTERVAL
            co2_gps = (m.joules_per_second * intensity) / 3600000
            co2_g = co2_gps * SAMPLE_INTERVAL

            total_co2 += co2_g
            total_joules += energy_j
            measurements_payload.append({
                "timestamp": m.timestamp.isoformat(),
                "joules_per_second": m.joules_per_second,
                "energy_j": energy_j,
                "co2_gps": co2_gps,
                "co2_g": co2_g,
                "intensity_g_per_kwh": intensity
            })

        results[country] = {
            "co2_g": total_co2,
            "energy_j": total_joules,
            "measurements": measurements_payload
        }

    return jsonify({
        "pod": pod,
        "container": container,
        "namespace": ns,
        "selection_mode": selection_mode,
        "measurement_count": len(selected_measurements),
        "start_time": selected_measurements[0].timestamp.isoformat() if selected_measurements else None,
        "end_time": selected_measurements[-1].timestamp.isoformat() if selected_measurements else None,
        "results": results
    })


@app.route('/api/co2-intensities', methods=['GET'])
def get_co2_intensities():
    logging.info(f"API Route: Current DE value is {co2_manager.intensities.get('DE')}")
    return jsonify(co2_manager.intensities)

@app.route('/healthz', methods=['GET'])
def healthz():
    return jsonify({
        "status": "ok",
        "started_at": health_state["started_at"].isoformat()
    }), 200

@app.route('/readyz', methods=['GET'])
def readyz():
    reasons = []
    now = datetime.now(timezone.utc)

    with health_lock:
        startup_check_completed = health_state["startup_check_completed"]
        startup_check_ok = health_state["startup_check_ok"]
        startup_errors = list(health_state["startup_errors"])
        last_success = health_state["last_kepler_scrape_success"]
        last_error = health_state["last_kepler_scrape_error"]

    if not startup_check_completed:
        reasons.append("startup_check_not_completed")
    if startup_check_completed and not startup_check_ok:
        reasons.extend(startup_errors)

    if not background_thread or not background_thread.is_alive():
        reasons.append("background_thread_not_alive")
    if not co2_thread or not co2_thread.is_alive():
        reasons.append("co2_thread_not_alive")
    if MODE == "db" and (not db_thread or not db_thread.is_alive()):
        reasons.append("db_thread_not_alive")
    if MODE == "db" and (not db_manager or not db_manager.connection):
        reasons.append("db_not_connected")

    max_staleness = SAMPLE_INTERVAL * 3 + 5
    if last_success is None:
        reasons.append("no_successful_kepler_scrape_yet")
    else:
        age = (now - last_success).total_seconds()
        if age > max_staleness:
            reasons.append(f"kepler_scrape_stale_{int(age)}s")

    status = 200 if not reasons else 503
    return jsonify({
        "ready": status == 200,
        "reasons": reasons,
        "last_kepler_scrape_success": last_success.isoformat() if last_success else None,
        "last_kepler_scrape_error": last_error.isoformat() if last_error else None
    }), status

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    run_startup_self_check()

    background_thread = threading.Thread(target=background_loop, daemon=True, name="background_loop")
    co2_thread = threading.Thread(target=co2_loop, daemon=True, name="co2_loop")
    if MODE == "db" and db_manager:
        db_thread = threading.Thread(target=db_loop, daemon=True, name="db_loop")
    background_thread.start()
    co2_thread.start()
    if db_thread:
        db_thread.start()
    
    logging.info("FLASK: Starting server on port 5001...")
    app.run(host='0.0.0.0', port=5001)
