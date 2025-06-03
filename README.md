# Use instructions steps

## Set-Up on Kubernetes Cluster

1.  Pull _co2-api_ image from docker hub under tuzgere1/co2-api:v0.0:
 ```
    docker image pull tuzgere1/co2-api:v0.0
 ``` 
2.  Service assumes image name to be co2-api so please rename as such using:
 ```
    docker tag tuzgere1/co2-api:v0.0 co2-api
 ```
3. Download needed yaml files available by email from _ivo.hrib@gmail.com_
- flask-service.yaml
- clusterrole.yaml
- cluster-role-binding.yaml
- flask-deployment.yaml
4. You can then load the image locally into the cluster OR alternatively directly pull it into the cluster and rename it afterwards (**depends on cluster management tool**)
    - Local example for KinD **(Tested)**:
        ```
            kind load docker-image co2-api --name <cluster-name>
        ```  

5. Run the following in the directory where files from step 3 were saved
```
    kubectl apply -f flask-service.yaml
    kubectl apply -f clusterrole.yaml
    kubectl apply -f cluster-role-binding.yaml
    kubectl apply -f flask-deployment.yaml
```
6. For ease of endpoint access you may forward the port as:
```
    kubectl port-forward pod/co2-api-*********** 5001:5001
```
replacing ********** with however it appears in:
```
    kubectl get pods
```

## API Calls
**Keep in mind, service takes about 30 sec to 1 min to fully start -> Avoid calls during this time.**

Service currently supports following countries marked by their appropriate iso2 code **(MORE TO BE ADDED EASILY GOING FORWARD)**:
- ["DE", "FR", "IT", "ES", "GB", "PL", "NL", "BE", "AT", "SE", "SI"]  

Range calls should be for at most within the last 1h 30 min, as currently only 2 hours of data are stored locally. Later implementation will save data to DB, allowing for same format but wider ranges 

**BEWARE OF TIMESTAMPS:** ALL timestamps are in GMT, and so may not match your local time.

If port forwarding freely use localhost instead of your-server-ip from here on out.

Power measurements are in kWh, co2 emissions measurements are in grams. Powwer "measurements" are executed every 10 seconds, adjustable later.

Output tests should yield JSON of different formats, although all fairly simple. If help in interpretation is needed please contact us.

### 1. Get Containers
**Endpoint:** `/containers`  
**Method:** `GET`  
**Description:** Retrieves a list of container IDs currently being tracked for power consumption.

**Example Query:**
```
curl -X GET http://<your-server-ip>:5001/api/containers
```

### 2. Get Power Consumption
**Endpoint:** `/power-consumption`  
**Method:** `POST`  
**Description:** Retrieves the last `n` power consumption readings for a specified container. TODO: Scaphandre measurements, currently generates gaussian data for each container 

**Request Body:**
```json
{
  "container_id": "container1",
  "n": 5
}
```

**Example Query:**
```
curl -X POST http://<your-server-ip>:5001/api/power-consumption
Content-Type: application/json

{
  "container_id": "container1",
  "n": 5
}
```

### 3. CO2 Per Container
**Endpoint:** `/co2-per-container`  
**Method:** `POST`  
**Description:** Calculates the CO2 emissions for a specified container based on its power consumption and the CO2 intensity as if it were in the specified country.

**Request Body:**
```json
{
  "container_id": "container1",
  "country_iso2": "DE",
  "n": 5
}
```

**Example Query:**
```
POST http://<your-server-ip>:5001/api/co2-per-container
Content-Type: application/json

{
  "container_id": "container1",
  "country_iso2": "DE",
  "n": 5
}
```

### 4. Get CO2 Intensities
**Endpoint:** `/co2-intensities`  
**Method:** `GET`  
**Description:** Retrieves the latest CO2 intensity values for all tracked countries.

**Example Query:**
```
curl -X GET http://<your-server-ip>:5001/api/co2-intensities
```

### 5. CO2 Range
**Endpoint:** `/co2-range`  
**Method:** `POST`  
**Description:** Retrieves CO2 emissions for a specified container within a given time range as if it were in the specified country.

**Request Body:**
```json
{
  "container_id": "container1",
  "country_iso2": "DE",
  "start_time": "2023-10-01T00:00:00",
  "end_time": "2023-10-01T01:00:00"
}
```

**Example Query:**
```
curl -X POST http://<your-server-ip>:5001/api/co2-range
Content-Type: application/json

{
  "container_id": "container1",
  "country_iso2": "DE",
  "start_time": "2023-10-01T00:00:00",
  "end_time": "2023-10-01T01:00:00"
}
```
### For any questions please contact _ivo.hrib@gmail.com_.

    