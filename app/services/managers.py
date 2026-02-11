import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from ..models import DataBlock, PowerMeasurement
from ..utils.config import SAMPLE_INTERVAL, DEFAULT_CO2_INTENSITIES
import logging


class TimeSeriesManager:
    def __init__(self, on_block_complete=None):
        self.blocks = defaultdict(lambda: deque(maxlen=10))
        self.current_block = {}
        self.last_joules = {}
        self.lock = threading.Lock()
        self.on_block_complete = on_block_complete

    def add_measurement(self, pod, container, ns, joules, timestamp):
        key = (pod, container, ns)
        with self.lock:
            if key not in self.last_joules:
                self.last_joules[key] = joules
                return

            diff = joules - self.last_joules[key]
            self.last_joules[key] = joules
            if diff < 0:
                logging.warning(f"Counter reset detected for {key}; skipping negative delta.")
                return

            jps = diff / SAMPLE_INTERVAL

            if key not in self.current_block:
                self.current_block[key] = DataBlock(timestamp, timestamp, [], 5)
            
            block = self.current_block[key]
            block.end_time = timestamp
            block.measurements.append(PowerMeasurement(timestamp, jps, ns, joules))
            
            if len(block.measurements) >= 5:
                block.is_complete = True
                self.blocks[key].append(block)
                del self.current_block[key]
                if self.on_block_complete:
                    try:
                        self.on_block_complete(key, block)
                    except Exception as e:
                        logging.error(f"Block completion callback failed for {key}: {e}")

class CO2IntensityManager:
    HISTORY_RETENTION_HOURS = 5

    def __init__(self, ready_event=None, max_workers=2):
        self.intensities = DEFAULT_CO2_INTENSITIES.copy()
        self.intensity_history = defaultdict(list)
        self.lock = threading.Lock()
        self.ready_event = ready_event
        self.max_workers = max_workers

        seed_time = datetime.now(timezone.utc)
        for country, val in self.intensities.items():
            self.intensity_history[country].append((seed_time, val))

    def _append_history(self, country, timestamp, value):
        history = self.intensity_history[country]
        history.append((timestamp, value))
        cutoff = timestamp - timedelta(hours=self.HISTORY_RETENTION_HOURS)
        self.intensity_history[country] = [(ts, val) for ts, val in history if ts >= cutoff]

    def update_intensities(self, scraper_func):
        countries = list(self.intensities.keys())
        logging.info(f"CO2 THREAD: Starting sequential scrape for {len(countries)} countries.")

        for country in countries:
            previous_intensity = self.intensities.get(country)
            updated = False
            for attempt in range(1, 4):
                try:
                    val = scraper_func(country)
                    if val is not None:
                        update_time = datetime.now(timezone.utc)
                        with self.lock:
                            self.intensities[country] = val
                            self._append_history(country, update_time, val)
                        logging.info(f"Updated {country}: {val}g (attempt {attempt})")
                        updated = True
                        break

                    logging.warning(
                        f"Scrape returned no value for {country} on attempt {attempt}. Retrying..."
                    )
                except Exception as e:
                    logging.error(f"Scrape failed for {country} on attempt {attempt}: {e}")

                if attempt < 3:
                    time.sleep(2)

            if not updated:
                fallback_intensity = previous_intensity
                if fallback_intensity is None:
                    fallback_intensity = DEFAULT_CO2_INTENSITIES.get(country, 0.0)

                with self.lock:
                    self.intensities[country] = fallback_intensity
                    self._append_history(country, datetime.now(timezone.utc), fallback_intensity)

                logging.warning(
                    f"Using fallback intensity for {country}: {fallback_intensity}g after 3 failed attempts."
                )

        if self.ready_event:
            self.ready_event.set()

    def get_intensity(self, country, _timestamp=None):
        with self.lock:
            if _timestamp is not None:
                ts = _timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                history = self.intensity_history.get(country, [])
                for h_ts, h_val in reversed(history):
                    if h_ts <= ts:
                        return h_val
                return DEFAULT_CO2_INTENSITIES.get(country, self.intensities.get("DE", 0.0))

            return self.intensities.get(country, self.intensities.get("DE", 0.0))
                    
