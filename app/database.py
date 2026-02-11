import threading
import logging
import psycopg2
from typing import List
from datetime import datetime
from collections import defaultdict
from .models import DataBlock, PowerMeasurement

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None
        self.pending_blocks = []
        self.lock = threading.Lock()
        self.connection_lock = threading.Lock()
        
    def connect(self):
        try:
            self.connection = psycopg2.connect(self.dsn)
            self.connection.autocommit = True
            self.ensure_schema()
            logging.info("Connected to TimescaleDB")
        except Exception as e:
            logging.error(f"Failed to connect to TimescaleDB: {e}")

    def ensure_schema(self):
        if not self.connection:
            return
        with self.connection_lock:
            with self.connection.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS container_metrics (
                        time TIMESTAMPTZ NOT NULL,
                        pod_id TEXT NOT NULL,
                        namespace TEXT,
                        energy_consumption DOUBLE PRECISION NOT NULL,
                        container_name TEXT NOT NULL,
                        joules_total DOUBLE PRECISION NOT NULL
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_container_metrics_lookup
                    ON container_metrics (pod_id, container_name, namespace, time DESC);
                """)
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
                    cur.execute("""
                        SELECT create_hypertable(
                            'container_metrics',
                            'time',
                            if_not_exists => TRUE
                        );
                    """)
                except Exception as e:
                    logging.warning(f"TimescaleDB extension/hypertable setup skipped: {e}")

    def queue_block_for_storage(self, key, block):
        with self.lock:
            self.pending_blocks.append((key, block))

    def process_pending_blocks(self):
        if not self.connection or not self.pending_blocks:
            return
        
        with self.lock:
            blocks_to_process = self.pending_blocks[:]
            self.pending_blocks = []

        try:
            with self.connection_lock:
                with self.connection.cursor() as cur:
                    for (pod, container, ns), block in blocks_to_process:
                        for m in block.measurements:
                            cur.execute("""
                                INSERT INTO container_metrics (time, pod_id, namespace, energy_consumption, container_name, joules_total)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (m.timestamp, pod, ns, m.joules_per_second, container, m.joules_total))
            logging.info(f"Successfully flushed {len(blocks_to_process)} blocks to DB")
        except Exception as e:
            logging.error(f"DB Write error: {e}")
            with self.lock:
                self.pending_blocks.extend(blocks_to_process)

    def get_measurements(self, pod, container, namespace, start_time=None, end_time=None, n=None):
        if not self.connection:
            return []

        params = [pod, container, namespace]
        where_sql = "pod_id = %s AND container_name = %s AND namespace = %s"

        if start_time is not None and end_time is not None:
            where_sql += " AND time >= %s AND time <= %s"
            params.extend([start_time, end_time])
            order_sql = "ORDER BY time ASC"
        else:
            order_sql = "ORDER BY time DESC"
            if n is not None:
                params.append(n)

        query = f"""
            SELECT time, namespace, energy_consumption, joules_total
            FROM container_metrics
            WHERE {where_sql}
            {order_sql}
            {"LIMIT %s" if (start_time is None and end_time is None and n is not None) else ""}
        """

        with self.connection_lock:
            with self.connection.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        measurements = [
            PowerMeasurement(
                timestamp=row[0],
                namespace=row[1],
                joules_per_second=row[2],
                joules_total=row[3]
            )
            for row in rows
        ]

        if start_time is None and end_time is None:
            measurements.reverse()

        return measurements

class MockDatabaseManager(DatabaseManager):
    def __init__(self):
        super().__init__(dsn="mock_dsn")
        self.mock_db_data = defaultdict(list)
        self.mock_co2_intensities = defaultdict(dict)

    def insert_co2_intensity(self, country, timestamp, intensity):
        self.mock_co2_intensities[country][timestamp] = intensity

    def get_co2_intensity_from_db(self, country, timestamp):
        data = self.mock_co2_intensities.get(country, {})
        valid_times = sorted([t for t in data.keys() if t <= timestamp], reverse=True)
        return data[valid_times[0]] if valid_times else None
