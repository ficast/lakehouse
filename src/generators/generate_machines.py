import pandas as pd
import random
from faker import Faker
import os

fake = Faker("en_US")

NUM_MACHINES = 200
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "machines.csv")

types = ["robot_arm", "press", "paint_gun", "conveyor", "welding_unit"]
manufacturers = ["ABB", "Siemens", "Fanuc", "KUKA", "Mitsubishi"]
locations = ["Assembly_Line_1", "Assembly_Line_2", "Painting_Zone", "Quality_Control", "Packaging_Area"]

machines = []
for i in range(NUM_MACHINES):
    machine_type = random.choice(types)
    machines.append({
        "machine_id": f"MCH-{i+1:04d}",
        "type": machine_type,
        "model": f"{random.choice(manufacturers)}-{random.randint(100,999)}",
        "manufacturer": random.choice(manufacturers),
        "location": random.choice(locations),
        "install_date": fake.date_between(start_date="-5y", end_date="-1y"),
        "max_temperature": random.uniform(70, 120),
        "max_vibration": random.uniform(2.0, 5.0),
        "max_energy_kw": random.uniform(3.0, 8.0)
    })

df = pd.DataFrame(machines)
df.to_csv(OUTPUT_PATH, index=False)

print(f"✅ Generated {len(df)} machines → {OUTPUT_PATH}")
