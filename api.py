from flask import Flask, request, jsonify
import threading
import time
import random
from kubernetes import client, config
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from collections import deque

app = Flask(__name__)

# Global WebDriver instance to maintain the driver state
driver = None

# Setup Selenium driver
def create_driver():
    global driver
    if driver is not None:
        driver.quit()
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--enable-unsafe-swiftshader")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# Initialize data
power_consumption_data = {}
co2_data = {}
tracked_countries = set()
lock = threading.Lock()

update_event = threading.Event()
is_updating = False

# List of European countries to track
default_countries = ["DE", "FR", "IT", "ES", "GB", "PL", "NL", "BE", "AT", "SE", "SI"]

def get_co2_data_for_country(country):
    """
    Fetch CO2 data for a given country, retrying up to 3 times if WebDriver fails.
    """
    retry_limit = 3
    retries = 0
    global driver
    while retries < retry_limit:
        try:
            # If the WebDriver is not initialized or was previously closed, create a new one
            if driver is None:
                driver = create_driver()

            # Navigate to the page
            url = f'https://app.electricitymaps.com/zone/{country}'
            driver.get(url)
            
            # Wait for the required element to be loaded
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.XPATH, "//p[@data-testid='co2-square-value']")))

            # If successfully loaded, parse the CO2 value
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            value_span = soup.find('p', {'data-testid': 'co2-square-value'}).find('span', class_='font-semibold')
            if value_span:
                co2_value = float(value_span.text.strip())
                print(f"Successfully fetched CO2 for {country}: {co2_value}")
                return co2_value
            else:
                raise Exception("CO2 value not found.")
        
        except Exception as e:
            # If any error occurs, increment the retry counter
            print(f"Attempt {retries + 1} failed for {country}: {e}")
            retries += 1
            # Restart WebDriver if any exception occurs
            driver.quit()
            driver = None  # Set the driver to None to force recreation on the next loop

            # If the retry limit is exceeded, raise the exception
            if retries == retry_limit:
                print(f"Failed to fetch CO2 data for {country} after {retry_limit} retries.")
                raise e

        # Add a short delay before retrying
        time.sleep(5)
    
    # Return None if unable to get data after retries
    return None

# Preload CO2 data for a list of countries at startup
def preload_co2_data():
    for country in default_countries:
        tracked_countries.add(country)
        try:
            co2_value = get_co2_data_for_country(country)
            if co2_value:
                hour_stamp = datetime.now().replace(minute=0, second=0, microsecond=0).isoformat()
                if country not in co2_data:
                    co2_data[country] = deque(maxlen=24)
                co2_data[country].append({"hour": hour_stamp, "value": co2_value})
                print(f"Preloaded CO2 data for {country}: {co2_value}")
        except Exception as e:
            print(f"Failed to preload CO2 for {country}: {e}")

# Get container IDs
def update_container_ids():
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        pods = v1.list_pod_for_all_namespaces(watch=False)
        with lock:
            for pod in pods.items:
                for container in pod.spec.containers:
                    if container.name not in power_consumption_data:
                        power_consumption_data[container.name] = deque(maxlen=720)
    except Exception as e:
        print(f"Could not load Kubernetes config: {e}")
        fallback = ['container1', 'container2', 'container3']
        with lock:
            for c in fallback:
                if c not in power_consumption_data:
                    power_consumption_data[c] = deque(maxlen=720)

# Generate fake power data
def generate_power(container_id):
    while True:
        value = max(0, random.gauss(30, 7))
        timestamp = datetime.now().isoformat()
        with lock:
            power_consumption_data[container_id].append({"timestamp": timestamp, "value": value})
        time.sleep(10)

def start_power_threads():
    while True:
        update_container_ids()
        with lock:
            for container_id in power_consumption_data.keys():
                if not any(t.name == container_id for t in threading.enumerate()):
                    t = threading.Thread(target=generate_power, args=(container_id,), name=container_id, daemon=True)
                    t.start()
        time.sleep(300)  # Check every 5 min for new containers

# Update CO2 data hourly
def update_co2_hourly():
    global is_updating
    while True:
        now = datetime.now()
        seconds_to_wait = 3600 - (now.minute * 60 + now.second)
        time.sleep(seconds_to_wait)

        print("Starting CO2 update...")
        is_updating = True
        update_event.clear()

        for country in list(tracked_countries):
            try:
                co2_value = get_co2_data_for_country(country)
                if co2_value is not None:
                    hour_stamp = datetime.now().replace(minute=0, second=0, microsecond=0).isoformat()
                    with lock:
                        if country not in co2_data:
                            co2_data[country] = deque(maxlen=4)
                        co2_data[country].append({"hour": hour_stamp, "value": co2_value})
                        print(f"Updated {country}: {co2_value}")
            except Exception as e:
                print(f"Failed to update CO2 for {country}: {e}")

        is_updating = False
        update_event.set()
        print("CO2 update finished.")

def get_co2_for_timestamp(country, timestamp_str):
    ts_hour = datetime.fromisoformat(timestamp_str).replace(minute=0, second=0, microsecond=0).isoformat()
    for record in reversed(co2_data.get(country, [])):
        if record["hour"] == ts_hour:
            return record["value"]
    return None

@app.route('/api/containers', methods=['GET'])
def get_containers():
    with lock:
        return jsonify(list(power_consumption_data.keys()))

@app.route('/api/power-consumption', methods=['POST'])
def get_power():
    data = request.json
    container_id = data.get('container_id')
    n = data.get('n', 5)
    with lock:
        if container_id in power_consumption_data:
            return jsonify(list(power_consumption_data[container_id])[-n:])
    return jsonify({"error": "Container not found"}), 404

@app.route('/api/co2-per-container', methods=['POST'])
def co2_per_container():
    data = request.json
    container_id = data.get('container_id')
    country = data.get('country_iso2')
    n = data.get('n', 5)
    tracked_countries.add(country)

    with lock:
        if container_id not in power_consumption_data:
            return jsonify({"error": "Container not found"}), 404

        measurements = list(power_consumption_data[container_id])[-n:]
        results = []
        for m in measurements:
            watts = m["value"]
            ts = m["timestamp"]
            co2_intensity = get_co2_for_timestamp(country, ts)
            if co2_intensity is None:
                continue
            grams_co2 = watts * co2_intensity
            results.append({
                "timestamp": ts,
                "kWh": round(watts, 3),
                "co2_g_per_kwh": co2_intensity,
                "grams_co2": round(grams_co2, 3)
            })
    return jsonify(results)

# New endpoint to check stored CO2 intensities for all tracked countries
@app.route('/api/co2-intensities', methods=['GET'])
def get_co2_intensities():
    with lock:
        result = {country: co2_data[country][-1] if co2_data[country] else None for country in tracked_countries}
    return jsonify(result)

@app.route('/api/co2-range', methods=['POST'])
def co2_range():
    data = request.json
    container_id = data.get('container_id')
    country = data.get('country_iso2')
    start_time_str = data.get('start_time')
    end_time_str = data.get('end_time')

    if not container_id or not country or not start_time_str or not end_time_str:
        return jsonify({"error": "Missing container_id, country_iso2, start_time, or end_time"}), 400

    try:
        start_time = datetime.fromisoformat(start_time_str)
        end_time = datetime.fromisoformat(end_time_str)
    except Exception as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    if start_time >= end_time:
        return jsonify({"error": "start_time must be before end_time"}), 400

    with lock:
        if container_id not in power_consumption_data:
            return jsonify({"error": "Container not found"}), 404

        results = []
        for m in power_consumption_data[container_id]:
            ts = datetime.fromisoformat(m["timestamp"])
            if start_time <= ts <= end_time:
                watts = m["value"]
                co2_intensity = get_co2_for_timestamp(country, m["timestamp"])
                if co2_intensity is not None:
                    grams_co2 = watts * co2_intensity
                    results.append({
                        "timestamp": m["timestamp"],
                        "kWh": round(watts, 3),
                        "co2_g_per_kwh": co2_intensity,
                        "grams_co2": round(grams_co2, 3)
                    })

    return jsonify(results)

if __name__ == '__main__':
    preload_co2_data()  # Preload CO2 data for default countries
    threading.Thread(target=start_power_threads, daemon=True).start()
    threading.Thread(target=update_co2_hourly, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
