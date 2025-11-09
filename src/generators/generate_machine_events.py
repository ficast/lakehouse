import json
import time
import random
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv() 

# Load static machine data
machines = pd.read_csv("data/raw/machines.csv")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
if not KAFKA_BOOTSTRAP:
    raise SystemExit("KAFKA_BOOTSTRAP não definido. Exporte a variável ou crie um .env com KAFKA_BOOTSTRAP=kafka:9092")

print(f"Using Kafka bootstrap server: {KAFKA_BOOTSTRAP}")

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Initialize machine states
machine_states = {
    row["machine_id"]: {
        "temperature": row["max_temperature"] * random.uniform(0.6, 0.8),
        "vibration": row["max_vibration"] * random.uniform(0.6, 0.8),
        "energy_kw": row["max_energy_kw"] * random.uniform(0.6, 0.8),
        "status": "OK",
    }
    for _, row in machines.iterrows()
}

def update_sensor(value, max_val, volatility=0.03):
    """
    Simula pequenas flutuações naturais do sensor com tendência à média.
    """
    drift = random.uniform(-volatility, volatility)
    new_val = value * (1 + drift)
    return max(0, min(new_val, 1.3 * max_val))

def evaluate_status(temp, vib, row):
    if temp > row["max_temperature"] or vib > row["max_vibration"]:
        return "FAILURE"
    elif temp > 0.9 * row["max_temperature"] or vib > 0.9 * row["max_vibration"]:
        # Pequena chance de alerta falso
        return "ALERT" if random.random() < 0.8 else "OK"
    elif random.random() < 0.01:
        # 1% chance de gerar alerta aleatório
        return "ALERT"
    else:
        return "OK"

print("🚀 Streaming realistic machine events to Kafka...")

while True:
    row = machines.sample(1).iloc[0]
    m_id = row["machine_id"]
    state = machine_states[m_id]

    # Atualiza sensores com variações pequenas
    state["temperature"] = update_sensor(state["temperature"], row["max_temperature"])
    state["vibration"] = update_sensor(state["vibration"], row["max_vibration"])
    state["energy_kw"] = update_sensor(state["energy_kw"], row["max_energy_kw"])

    # Avalia status com base em thresholds
    status = evaluate_status(state["temperature"], state["vibration"], row)
    state["status"] = status

    event = {
        "event_id": f"evt-{random.randint(100000, 999999)}",
        "machine_id": m_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(state["temperature"], 2),
        "vibration": round(state["vibration"], 2),
        "energy_kw": round(state["energy_kw"], 2),
        "status": status,
        "production_count": random.randint(1, 5) if status == "OK" else 0,
    }

    producer.send("machine_events", event)
    print(f"➡️ {event}")
    time.sleep(random.uniform(0.3, 1.2))
