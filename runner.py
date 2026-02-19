import time
import subprocess

while True:
    subprocess.run(["python", "load_realtime_vehicle_positions.py"], check=False)
    print("Data Loaded")
    time.sleep(60)
