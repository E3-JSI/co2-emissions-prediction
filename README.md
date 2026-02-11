# CO2 Emissions Monitoring API

This Flask API monitors container power consumption and calculates CO2 emissions for selected countries. It supports local in-memory mode and persistent TimescaleDB-backed mode.

![Description](images/arhitecture.png)

## Table of Contents
- [CO2 Emissions Monitoring API](#co2-emissions-monitoring-api)
  - [Table of Contents](#table-of-contents)
  - [API Endpoints](#api-endpoints)
    - [`POST /api/co2-per-container`](#post-apico2-per-container)
    - [`GET /api/containers`](#get-apicontainers)
    - [`GET /api/co2-intensities`](#get-apico2-intensities)
    - [`GET /healthz`](#get-healthz)
    - [`GET /readyz`](#get-readyz)
  - [Environment Variables](#environment-variables)
  - [Mode Behavior (`local` vs `db`)](#mode-behavior-local-vs-db)
    - [`MODE=local`](#modelocal)
    - [`MODE=db`](#modedb)
  - [Local Development and Testing](#local-development-and-testing)
    - [Running Locally](#running-locally)
    - [Running in DB Mode Locally](#running-in-db-mode-locally)
  - [Kubernetes Deployment](#kubernetes-deployment)
    - [Prerequisites](#prerequisites)
    - [Creating the `TSDB_DSN` Secret](#creating-the-tsdb_dsn-secret)
    - [Building and Pushing the Docker Image](#building-and-pushing-the-docker-image)
    - [Deploying to Kubernetes](#deploying-to-kubernetes)
    - [Kepler Endpoint Configuration](#kepler-endpoint-configuration)

---

## API Endpoints

### `POST /api/co2-per-container`
Retrieves CO2 emissions data per container, either for a time range or the last N measurements.

* **Parameters (JSON Body):**
  * `pod` (required, string)
  * `container` (required, string)
  * `namespace` (required, string)
  * `countries` (optional, array of strings, default `["DE"]`)
  * `start_time` (optional, ISO 8601 string)
  * `end_time` (optional, ISO 8601 string)
  * `n` (optional, integer, default `5`)
* **Rules:**
  * Provide both `start_time` and `end_time` for range mode.
  * If range fields are omitted, last-N mode is used.
* **Response:**
  * Top-level metadata (`selection_mode`, `measurement_count`, selected time bounds)
  * Per-country totals (`co2_g`, `energy_j`)
  * Per-measurement values (`timestamp`, `joules_per_second`, `co2_gps`, `co2_g`, `intensity_g_per_kwh`)
* **`curl` Example (Time Range):**
  ```bash
  curl -X POST -H "Content-Type: application/json" -d '{
    "pod": "busybox-worker",
    "container": "busybox",
    "namespace": "default",
    "countries": ["DE", "FR"],
    "start_time": "2026-02-11T10:00:00Z",
    "end_time": "2026-02-11T10:10:00Z"
  }' http://localhost:5001/api/co2-per-container
  ```
* **`curl` Example (Last N Measurements):**
  ```bash
  curl -X POST -H "Content-Type: application/json" -d '{
    "pod": "busybox-worker",
    "container": "busybox",
    "namespace": "default",
    "countries": ["DE"],
    "n": 10
  }' http://localhost:5001/api/co2-per-container
  ```

### `GET /api/containers`
Returns all currently known `(pod, container, namespace)` tuples.

* **`curl` Example:**
  ```bash
  curl http://localhost:5001/api/containers
  ```

### `GET /api/co2-intensities`
Returns latest in-memory CO2 intensity values by country code.

* **`curl` Example:**
  ```bash
  curl http://localhost:5001/api/co2-intensities
  ```

### `GET /healthz`
Liveness endpoint.

* **`curl` Example:**
  ```bash
  curl http://localhost:5001/healthz
  ```

### `GET /readyz`
Readiness endpoint. Reports startup checks, scraper freshness, and in `db` mode also DB thread/connection health.

* **`curl` Example:**
  ```bash
  curl http://localhost:5001/readyz
  ```

---

## Environment Variables

* `MODE` (string): `local` or `db`. Default: `local`.
* `SAMPLE_INTERVAL` (int): Kepler scrape interval in seconds. Default: `10`.
* `DB_FLUSH_INTERVAL` (int): DB queue flush interval in seconds (used in `db` mode). Default: `5`.
* `EXPORTER_URL` (string): Kepler metrics endpoint. Default: `http://localhost:9102/metrics`.
* `TSDB_DSN` (string): PostgreSQL/TimescaleDB DSN. Required in `MODE=db`.
* `CHROME_BIN` (string): Optional Chromium binary path.
* `CHROMEDRIVER_BIN` (string): Optional Chromedriver path.

---

## Mode Behavior (`local` vs `db`)

### `MODE=local`
* Measurements are kept in in-memory rolling buffers.
* API queries are served from memory only.
* Data is lost on restart.

### `MODE=db`
* Completed measurement blocks are queued and flushed to DB by a background thread.
* `/api/co2-per-container` reads historical data from DB and merges current in-progress in-memory points.
* Schema bootstrap is automatic on connect:
  * creates `container_metrics` table
  * creates lookup index
  * attempts Timescale extension + hypertable (falls back to normal PostgreSQL table if unavailable)

**DB safety note:** duplicate prevention is best handled at DB level (unique key + `ON CONFLICT`) if strict idempotency is required.

---

## Local Development and Testing

### Running Locally

1. Clone and enter repo.
2. Create and activate a virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirement.txt
   ```
4. Run:
   ```bash
   MODE=local SAMPLE_INTERVAL=10 python -m app.main
   ```

### Running in DB Mode Locally

1. Start PostgreSQL/TimescaleDB.
2. Export DSN and mode:
   ```bash
   MODE=db
   TSDB_DSN=postgresql://postgres:postgres@localhost:5432/co2
   SAMPLE_INTERVAL=10
   DB_FLUSH_INTERVAL=5
   python -m app.main
   ```
3. Verify:
   * `GET /readyz` should return `200`.
   * Query `container_metrics` to confirm inserts.

---

## Kubernetes Deployment

### Prerequisites

* Kubernetes cluster
* `kubectl` configured
* Kepler deployed and reachable
* PostgreSQL/TimescaleDB reachable from cluster (if using `MODE=db`)

### Creating the `TSDB_DSN` Secret

1. Encode DSN:
   ```bash
   echo -n "postgresql://user:password@host:5432/co2" | base64
   ```
2. Create secret manifest:
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: co2-db-secret
   type: Opaque
   data:
     TSDB_DSN: <base64-encoded-dsn>
   ```
3. Apply:
   ```bash
   kubectl apply -f db-secret.yaml
   ```

### Building and Pushing the Docker Image

```bash
docker build -t your-repo/co2-api:latest .
docker push your-repo/co2-api:latest
```

### Deploying to Kubernetes

Use `k8s/deployment.yaml` and set environment values:

* Local mode:
  * `MODE=local`
* DB mode:
  * `MODE=db`
  * `TSDB_DSN` from Kubernetes secret

Apply:
```bash
kubectl apply -f k8s/deployment.yaml
kubectl get pods -l app=co2-api
kubectl get svc co2-api-service
```

### Kepler Endpoint Configuration

Set `EXPORTER_URL` to your Kepler metrics service URL inside the cluster (for example):

```text
http://kepler.kepler.svc.cluster.local:9102/metrics
```

Verify the service in your cluster:
```bash
kubectl get svc -A | grep -i kepler
```
