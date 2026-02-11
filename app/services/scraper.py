import requests
import logging
import re
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from prometheus_client.parser import text_string_to_metric_families
from ..utils.config import EXPORTER_URL, CHROME_BIN, CHROMEDRIVER_BIN
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_selenium_driver():
    options = Options()
    if CHROME_BIN:
        options.binary_location = CHROME_BIN
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    if CHROMEDRIVER_BIN:
        return webdriver.Chrome(service=Service(CHROMEDRIVER_BIN), options=options)
    return webdriver.Chrome(options=options)

def scrape_kepler_metrics(ts_manager):
    try:
        resp = requests.get(EXPORTER_URL, timeout=5)
        resp.raise_for_status()
        now = datetime.now(timezone.utc)
        aggregated_joules = {}
        series_count = 0
        
        for fam in text_string_to_metric_families(resp.text):
            if "kepler_container_joules" in fam.name:
                for sample in fam.samples:
                    pod = sample.labels.get("pod_name")
                    container = sample.labels.get("container_name")
                    ns = sample.labels.get("container_namespace")
                    mode = sample.labels.get("mode")
                    
                    if mode == "dynamic" and pod and container:
                        key = (pod, container, ns)
                        aggregated_joules[key] = aggregated_joules.get(key, 0.0) + float(sample.value)
                        series_count += 1

        for (pod, container, ns), joules_total in aggregated_joules.items():
            ts_manager.add_measurement(pod, container, ns, joules_total, now)
                        
        logging.info(
            f"KEPLER: Aggregated {series_count} dynamic series into {len(aggregated_joules)} containers."
        )
        return True
    except Exception as e:
        logging.error(f"KEPLER: Scrape failed: {e}")
        return False



def scrape_electricity_maps(country_code):
    driver = None
    try:
        driver = get_selenium_driver()
        url = f"https://app.electricitymaps.com/zone/{country_code}"
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "p[data-testid='co2-square-value']")))
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        container = soup.find('p', {'data-testid': 'co2-square-value'})
        if container:
            span = container.find('span', class_='font-semibold')
            if span:
                match = re.search(r"-?\d+(?:\.\d+)?", span.text.strip())
                if not match:
                    return None
                val = float(match.group(0))
                logging.info(f"Successfully scraped {country_code}: {val}g")
                return val
    except Exception as e:
        logging.error(f"Selenium failed for {country_code}: {str(e)[:100]}")
    finally:
        if driver:
            driver.quit()
    return None
