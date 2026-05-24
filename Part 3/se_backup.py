import json
import warnings
warnings.filterwarnings("ignore")
import gzip
import os
import time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "cs536-data-engineering"
SUBSCRIPTION_ID = "se_backup_sub"
BACKUP_DIR = "/home/vinodh"

def process_day(subscriber, sub_path):
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(BACKUP_DIR, f"se_{today}.json.gz")
    vehicle_ids = set()
    total = 0
    begin_wall = None
    sentinel_found = False
    total_count = None
    with gzip.open(filepath, "wt") as f:
        while True:
            try:
                response = subscriber.pull(request={"subscription": sub_path, "max_messages": 500}, timeout=10)
            except Exception as e:
                if "504" in str(e) or "Deadline" in str(e):
                    time.sleep(2)
                    continue
                else:
                    raise
            if not response.received_messages:
                if sentinel_found and total_count is not None and total >= total_count:
                    break
                time.sleep(2)
                continue
            ack_ids = []
            for msg in response.received_messages:
                ack_ids.append(msg.ack_id)
                data = json.loads(msg.message.data.decode("utf-8"))
                if data.get("sentinel") or msg.message.attributes.get("sentinel") == "true":
                    sentinel_found = True
                    total_count = data.get("total_count")
                    continue
                if begin_wall is None:
                    begin_wall = datetime.now()
                vid = data.get("vehicle_number")
                if vid is not None:
                    vehicle_ids.add(vid)
                f.write(json.dumps(data) + chr(10))
                total += 1
            if ack_ids:
                subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
            if sentinel_found and total_count is not None and total >= total_count:
                break
    end_wall = datetime.now()
    raw_size = os.path.getsize(filepath)
    elapsed = (end_wall - begin_wall).total_seconds() if begin_wall else 0
    throughput = total / elapsed if elapsed > 0 else 0
    print(f"BEGIN_TIMESTAMP: {begin_wall.isoformat() if begin_wall else chr(78)+chr(47)+chr(65)}")
    print(f"NUM_RECORDS:     {total}")
    print(f"FILESIZE:        {raw_size} bytes")
    print(f"NUM_VEHICLES:    {len(vehicle_ids)}")
    print(f"END_TIMESTAMP:   {end_wall.isoformat()}")
    print(f"WALLTIME:        {elapsed:.2f}s")
    print(f"THROUGHPUT:      {throughput:.1f} records/sec")

def main():
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    print("se_backup.py started, waiting for data...")
    while True:
        try:
            process_day(subscriber, sub_path)
            print("Day complete. Waiting for next days data...")
        except Exception as e:
            if "504" in str(e) or "Deadline" in str(e):
                time.sleep(10)
            else:
                print(f"Error: {e} - retrying in 10s")
                time.sleep(10)

if __name__ == "__main__":
    main()