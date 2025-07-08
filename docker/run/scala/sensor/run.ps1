docker run --rm `
  -v ./volumes/application.conf:/application.conf `
  -v ./volumes/checkpoints:/checkpoints `
  -e KAFKA_TOPIC=sensors `
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 `
  -e KAFKA_SCHEMA_REGISTRY_URL=http://schema-registry:8081 `
  --network mdwh-network `
  mdwh/sensor-scala:latest TH0101
