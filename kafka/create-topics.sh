#!/bin/bash
set -e

echo "📦 Waiting for Kafka to be ready..."
sleep 5


for t in machine_events; do
  echo "🧠 Checking if topic '$t' exists..."

  EXISTS=$(kafka-topics --list --bootstrap-server localhost:9092 | grep -w $t || true)

  if [ -z "$EXISTS" ]; then
    kafka-topics --create --if-not-exists \
      --bootstrap-server localhost:9092 \
      --topic $t \
      --partitions 3 \
      --replication-factor 1
  else
    echo "✅ Topic '$t' already exists"
  fi
done

echo "🏁 Kafka initialization complete!"
