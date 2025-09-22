```cmd
./docker-compose/volumes/files/sensors/application.conf
```

```cmd
docker compose up -d
```

Minio create a bucket `lakehouse`

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/sensors-sink.json localhost:8083/connectors/sensors-sink/config | jq
```

Postgres
```sql
CREATE DATABASE operational_data;

CREATE TABLE locations (
	location_id bigint PRIMARY KEY,
	name varchar NOT NULL,
	description text
)

CREATE TABLE devices (
	device_id bigint PRIMARY KEY,
	device_code varchar UNIQUE NOT NULL,
  	model varchar,
  	description text
)

CREATE TABLE device_installations (
  installation_id bigint PRIMARY KEY,
  device_id bigint REFERENCES devices(device_id),
  location_id bigint REFERENCES locations(location_id),
  installed_at timestamp NOT NULL,
  removed_at timestamp
)

INSERT INTO locations VALUES 
	(1, 'Laboratory 1', 'Building 1, Room 01'),
	(2, 'Laboratory 2', 'Building 1, Room 02');
	

INSERT INTO devices VALUES
	(1, 'TH0101', 'Model TH', 'Temperature and humidity sensor'),	
	(2, 'T0102', 'Model T', 'Temperature sensor'),
	(3, 'H0102', 'Model H', 'Humidity sensor');
	

INSERT INTO device_installations VALUES 
	(1, 1, 1, '2025-01-01', null),
	(2, 2, 2, '2025-01-01', null),
	(3, 3, 2, '2025-01-01', null);
```

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/postgres-source-cdc-flatten.json localhost:8083/connectors/postgres-source-cdc-flatten/config | jq
```

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/postgres-source-cdc.json localhost:8083/connectors/postgres-source-cdc/config | jq
```

```sql
INSERT INTO devices VALUES
	(4, 'TH0130', 'Model TH', 'Temperature and humidity sensor');

UPDATE devices
SET device_code = 'TH0103'
WHERE device_id = 4;

INSERT INTO devices VALUES
	(5, 'TH0177', 'Model TH', 'Temperature and humidity sensor');
	
DELETE FROM devices 
WHERE device_id = 5;
```

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/devices-sink.json localhost:8083/connectors/devices-sink/config | jq
```

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/locations-sink.json localhost:8083/connectors/locations-sink/config | jq
```

```bash
curl -s -X PUT -H "Content-Type: application/json" -d @./configs/kafka-connect/device-installations-sink.json localhost:8083/connectors/device-installations-sink/config | jq
```