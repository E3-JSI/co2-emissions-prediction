# CO2 Emissions Monitoring API

This Flask API monitors power consumption and calculates CO2 emissions from containers, providing both real-time data and historical analysis. It supports local in-memory mocking and persistent storage using TimescaleDB.

## Table of Contents
- [CO2 Emissions Monitoring API](#co2-emissions-monitoring-api)
  - [Table of Contents](#table-of-contents)
  - [API Endpoints](#api-endpoints)
    - [`POST /api/power-consumption`](#post-apipower-consumption)
    - [`POST /api/co2-per-container`](#post-apico2-per-container)
    - [`GET /api/containers`](#get-apicontainers)
    - [`GET /api/export-time-series`](#get-apiexport-time-series)
    - [`GET /api/co2-intensities`](#get-apico2-intensities)
    - [`POST /api/co2-range`](#post-apico2-range)
    - [`GET /health`](#get-health)
    - [`GET /api/status`](#get-apistatus)
  - [Environment Variables](#environment-variables)
  - [Local Development and Testing](#local-development-and-testing)
    - [Running Locally](#running-locally)
    - [Testing with Mock DB](#testing-with-mock-db)
  - [Kubernetes Deployment](#kubernetes-deployment)
    - [Prerequisites](#prerequisites)
    - [Creating the `TSDB_DSN` Secret](#creating-the-tsdb_dsn-secret)
    - [Building and Pushing the Docker Image](#building-and-pushing-the-docker-image)
    - [Deploying to Kubernetes](#deploying-to-kubernetes)
    - [Kepler Endpoint Configuration](#kepler-endpoint-configuration)

---

## API Endpoints

### `POST /api/power-consumption`
Retrieves recent power consumption data for all or specific containers.

*   **Parameters (JSON Body):**
    *   `n` (optional, integer): Number of most recent blocks to retrieve. Defaults to 5.
    *   `pod` (optional, string): Filter by pod name.
    *   `container` (optional, string): Filter by container name.
    *   `namespace` (optional, string): Filter by namespace.
*   **Response:** A JSON array of container data, each containing an array of `DataBlock` objects.
*   **`curl` Example (All containers, last 5 blocks):**
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{}' \\
    http://localhost:5001/api/power-consumption
    ```
*   **`curl` Example (Specific container, last 1 block):**
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "pod": "busybox-test-84656d96bf-5bs7p",
        "container": "busybox",
        "namespace": "default",
        "n": 1
    }' http://localhost:5001/api/power-consumption
    ```

### `POST /api/co2-per-container`
Retrieves CO2 emissions data per container for a specified time range or last N measurements, calculated for specified countries.

*   **Parameters (JSON Body):**
    *   `pod` (required, string): Filter by pod name.
    *   `container` (required, string): Filter by container name.
    *   `namespace` (required, string): Filter by namespace.
    *   `countries` (required, array of strings): List of ISO2 country codes (e.g., \["DE", "FR"]).
    *   `start_time` (optional, string): Start timestamp in ISO 8601 format (e.g., "2023-10-27T10:00:00Z").
    *   `end_time` (optional, string): End timestamp in ISO 8601 format.
    *   `n` (optional, integer): Number of most recent individual measurements to retrieve.
    *   **Note:** You must provide either (`start_time` and `end_time`) OR `n`, but not both.
*   **Response:** A JSON array of detailed measurement objects, each including:
    *   `timestamp` (string, ISO 8601)
    *   `joules_per_second` (float)
    *   `co2_emissions_by_country` (object, mapping country code to emissions data)
*   **`curl` Example (Time Range):**
    ```bash
    # Get current time for example (Python):
    # import datetime; from datetime import timezone, timedelta; now = datetime.now(timezone.utc); start = now - timedelta(minutes=1); print(f"Start: {start.isoformat()}, End: {now.isoformat()}")

    curl -X POST -H "Content-Type: application/json" -d '{
        "pod": "busybox-test-84656d96bf-5bs7p",
        "container": "busybox",
        "namespace": "default",
        "countries": ["DE", "FR"],
        "start_time": "2023-10-27T10:00:00Z",
        "end_time": "2023-10-27T10:01:00Z"
    }' http://localhost:5001/api/co2-per-container
    ```
*   **`curl` Example (Last N Measurements):**
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{
        "pod": "busybox-test-84656d96bf-5bs7p",
        "container": "busybox",
        "namespace": "default",
        "countries": ["DE"],
        "n": 5
    }' http://localhost:5001/api/co2-per-container
    ```

### `GET /api/containers`
Get a list of all monitored `(pod, container, namespace)` tuples.

*   **Response:** A JSON array of objects, each with `pod`, `container`, and `namespace` keys.
*   **`curl` Example:**
    ```bash
    curl http://localhost:5001/api/containers
    ```

### `GET /api/export-time-series`
Export all raw time series data (measurements within blocks).

*   **Response:** A JSON array of measurement objects, each with `pod`, `container`, `namespace`, `timestamp`, `joules_per_second`, `block_start`, and `block_end`.
*   **`curl` Example:**
    ```bash
    curl http://localhost:5001/api/export-time-series
    ```

### `GET /api/co2-intensities`
Return the latest known CO2 intensity per tracked country (hourly).

*   **Response:** A JSON array of objects, each with `country`, `hour` (ISO 8601), and `value`.
*   **`curl` Example:**
    ```bash
    curl http://localhost:5001/api/co2-intensities
    ```

### `POST /api/co2-range`
Alias for `/api/co2-per-container` with explicit `start_time` and `end_time` in the request body.

*   **Parameters:** Same as `/api/co2-per-container` (but typically used with `start_time` and `end_time`).
*   **Response:** Same as `/api/co2-per-container`.
*   **`curl` Example:**
    ```bash
    # Similar to the time range example for /api/co2-per-container
    ```

### `GET /health`
Health check endpoint.

*   **Response:** JSON object with `status`, `monitored_containers`, `total_blocks`, `current_blocks`, `mode`, and `timestamp`.
*   **`curl` Example:**
    ```bash
    curl http://localhost:5001/health
    ```

### `GET /api/status`
Detailed status endpoint for debugging.

*   **Response:** JSON object with `status`, `last_scrape_time`, `last_measurement_time`, `has_data`, `unique_container_keys`, `pending_db_blocks`, `mode`, and `timestamp`.
*   **`curl` Example:**
    ```bash
    curl http://localhost:5001/api/status
    ```

---

## Environment Variables

*   `MODE` (string): Set to `"db"` to enable TimescaleDB persistence. Defaults to `"local"` (uses `MockDatabaseManager`).
*   `TSDB_DSN` (string): TimescaleDB connection string. Required when `MODE` is `"db"`. Example: `postgresql://user:password@host:port/database`
*   `EXPORTER_URL` (string): URL of the Kepler Prometheus exporter. Defaults to `http://localhost:9102/metrics`. **In Kubernetes, this should be the service URL of your Kepler deployment.**

---

## Local Development and Testing

### Running Locally

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd co2
    ```
2.  **Create a Python virtual environment (recommended):**
    ```bash
    python -m venv venv
    # On Windows
    .\\venv\\Scripts\\activate
    # On Linux/macOS
    source venv/bin/activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    You will also need a Chrome/Chromium browser and its WebDriver (e.g., `chromedriver`) installed and accessible in your PATH for CO2 intensity scraping to work.
4.  **Run the application:**
    ```bash
    python sth/api2.py
    ```
    The API will be available at `http://localhost:5001`. By default, it will run in `local/mock DB mode`.

### Testing with Mock DB

To test the "db mode" logic while still using the in-memory `MockDatabaseManager` (useful for local development without a real database):

1.  **Set environment variables:**
    *   **Windows (PowerShell):**
        ```powershell
        $env:MODE="db"
        $env:TSDB_DSN=""
        ```
    *   **Linux/macOS (Bash/Zsh):**
        ```bash
        export MODE="db"
        export TSDB_DSN=""
        ```
2.  **Run the application:**
    ```bash
    python sth/api2.py
    ```
    You will see `INFO:root:Running in local/mock DB mode.`, but the application's logic will use the "db mode" code paths, interacting with the `MockDatabaseManager`.
3.  **Make `curl` requests:** Use the `curl` examples provided in the [API Endpoints](#api-endpoints) section to test. Give the application some time (e.g., 20-30 seconds after startup) for background scraping threads to populate initial data.

---

## Kubernetes Deployment

This section guides you through deploying the CO2 monitoring API to a Kubernetes cluster.

### Prerequisites

*   A running Kubernetes cluster.
*   `kubectl` configured to communicate with your cluster.
*   TimescaleDB (or compatible PostgreSQL) instance accessible from your cluster.
*   Kepler deployed in your cluster, with its Prometheus exporter accessible.
*   Docker or a compatible containerization tool.

### Creating the `TSDB_DSN` Secret

Your TimescaleDB connection string (`TSDB_DSN`) contains sensitive information. We will store it as a Kubernetes Secret.

1.  **Encode your `TSDB_DSN` string in Base64:**
    ```bash
    echo -n "postgresql://user:password@host:port/database" | base64
    ```
    Replace `"postgresql://user:password@host:port/database"` with your actual TimescaleDB connection string.
2.  **Create a `Secret` YAML file (e.g., `db-secret.yaml`):**
    ```yaml
    apiVersion: v1
    kind: Secret
    metadata:
      name: co2-db-secret
    type: Opaque
    data:
      TSDB_DSN: <your-base64-encoded-dsn>
    ```
    Replace `<your-base64-encoded-dsn>` with the output from the `base64` command.
3.  **Apply the secret to your cluster:**
    ```bash
    kubectl apply -f db-secret.yaml
    ```

### Building and Pushing the Docker Image

1.  **Ensure you have a `requirements.txt` file** containing all Python dependencies (Flask, requests, prometheus\_client, beautifulsoup4, selenium, psycopg2-binary, etc.). If not, create it:
    ```bash
    pip freeze > requirements.txt
    ```
2.  **Build the Docker image:**
    ```bash
    docker build -t your-docker-repo/co2-api:latest .
    ```
    Replace `your-docker-repo` with your Docker Hub username or private registry.
3.  **Push the Docker image to your registry:**
    ```bash
    docker push your-docker-repo/co2-api:latest
    ```

### Deploying to Kubernetes

Here's the `k8s-deployment.yaml` file for deploying the application. This includes a `Deployment` and a `Service`.

*(Note: Ensure Kepler is already deployed and its service is accessible within the cluster. Replace `kepler-exporter.kepler.svc.cluster.local:9102` with the actual service endpoint of your Kepler Prometheus exporter if it's different in your cluster.)*

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: co2-api-deployment
  labels:
    app: co2-api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: co2-api
  template:
    metadata:
      labels:
        app: co2-api
    spec:
      containers:
      - name: co2-api
        image: your-docker-repo/co2-api:latest # Replace with your image
        ports:
        - containerPort: 5001
        env:
        - name: MODE
          value: "db" # Enable DB mode for persistent storage
        - name: EXPORTER_URL
          value: "http://kepler-exporter.kepler.svc.cluster.local:9102/metrics" # Kepler service endpoint
        - name: TSDB_DSN # Reference the secret for the database connection string
          valueFrom:
            secretKeyRef:
              name: co2-db-secret
              key: TSDB_DSN
---
apiVersion: v1
kind: Service
metadata:
  name: co2-api-service
spec:
  selector:
    app: co2-api
  ports:
    - protocol: TCP
      port: 5001
      targetPort: 5001
  type: LoadBalancer # Use LoadBalancer for external access, or ClusterIP for internal only
```
1.  **Save the deployment YAML:** Save the above content as `k8s-deployment.yaml`.
2.  **Apply the deployment to your cluster:**
    ```bash
    kubectl apply -f k8s-deployment.yaml
    ```
3.  **Check deployment status:**
    ```bash
    kubectl get pods -l app=co2-api
    kubectl get svc co2-api-service
    ```
    Once the service is running, you can get its external IP (if using `LoadBalancer`) to access your API.

### Kepler Endpoint Configuration

**Important:** The `EXPORTER_URL` environment variable in the `k8s-deployment.yaml` is set to `http://kepler-exporter.kepler.svc.cluster.local:9102/metrics`. This is a common service endpoint for Kepler's Prometheus exporter within a Kubernetes cluster.

*   `kepler-exporter`: This is typically the name of the Kubernetes `Service` that exposes Kepler's metrics.
*   `kepler`: This is the namespace where Kepler is deployed.
*   `svc.cluster.local`: This is the default domain for Kubernetes services.

**Verify this endpoint in your cluster.** If your Kepler deployment uses a different service name, namespace, or port, you must update the `EXPORTER_URL` value in your `k8s-deployment.yaml` accordingly. You can typically find this information by running:
```bash
kubectl get svc -n <kepler-namespace>
```
and looking for the service exposing port 9102 (or similar).
