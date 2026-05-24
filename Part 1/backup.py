import json
import gzip
import os
import time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "cs536-data-engineering"
SUBSCRIPTION_ID = "backup_sub"
BACKUP_DIR = "/home/vinodh"

DRAIN_SECONDS = 30

def process_day(subscriber, sub_path):
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = os.path.join(BACKUP_DIR, f"breadcrumbs_{today}_.log")

    vehicle_ids = set()
    total = 0
    begin_wall = None

    with open(filepath, "w") as f:
        while True:
            try:
                response = subscriber.pull(
                    request={"subscription": sub_path, "max_messages": 500},
                    timeout=10,
                )
            except Exception as e:
                if "504" in str(e) or "Deadline Exceeded" in str(e):
                    time.sleep(2)
                    continue
                else:
                    raise

            if not response.received_messages:
                time.sleep(2)
                continue

            ack_ids = []
            sentinel_found = False

            for msg in response.received_messages:
                ack_ids.append(msg.ack_id)
                data = json.loads(msg.message.data.decode("utf-8"))

                if data.get("sentinel") or \
                   msg.message.attributes.get("sentinel") == "true":
                    sentinel_found = True
                    continue

                if begin_wall is None:
                    begin_wall = datetime.now()

                vid = data.get("VEHICLE_ID")
                if vid is not None:
                    vehicle_ids.add(vid)

                f.write(json.dumps(data) + "\n")
                total += 1

            if ack_ids:
                subscriber.acknowledge(
                    request={"subscription": sub_path, "ack_ids": ack_ids}
                )

            if sentinel_found:
                # Drain for 30 more seconds to catch late messages
                drain_until = time.time() + DRAIN_SECONDS
                while time.time() < drain_until:
                    try:
                        response = subscriber.pull(
                            request={"subscription": sub_path, "max_messages": 500},
                            timeout=10,
                        )
                        if not response.received_messages:
                            break
                        ack_ids = []
                        for msg in response.received_messages:
                            ack_ids.append(msg.ack_id)
                            data = json.loads(msg.message.data.decode("utf-8"))
                            if data.get("sentinel") or \
                               msg.message.attributes.get("sentinel") == "true":
                                continue
                            if begin_wall is None:
                                begin_wall = datetime.now()
                            vid = data.get("VEHICLE_ID")
                            if vid is not None:
                                vehicle_ids.add(vid)
                            f.write(json.dumps(data) + "\n")
                            total += 1
                        if ack_ids:
                            subscriber.acknowledge(
                                request={"subscription": sub_path, "ack_ids": ack_ids}
                            )
                    except Exception:
                        break

                # Close and compress after drain
                f.flush()
                raw_size = os.path.getsize(filepath)

                gz_path = filepath + ".gz"
                with open(filepath, "rb") as fin, gzip.open(gz_path, "wb") as fout:
                    fout.writelines(fin)
                os.remove(filepath)

                compress_ts = datetime.now()
                elapsed = (compress_ts - begin_wall).total_seconds() if begin_wall else 0
                throughput = total / elapsed if elapsed > 0 else 0

                print(f"BEGIN_TIMESTAMP: {begin_wall.isoformat() if begin_wall else 'N/A'}")
                print(f"NUM_BREADCRUMBS: {total}")
                print(f"DATASIZE:        {raw_size} bytes")
                print(f"NUM_VEHICLES:    {len(vehicle_ids)}")
                print(f"END_TIMESTAMP:   {compress_ts.isoformat()}")
                print(f"WALLTIME:        {elapsed:.2f}s")
                print(f"THROUGHPUT:      {throughput:.1f} breadcrumbs/sec")
                return

def main():
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print("backup.py started, waiting for data...")
    while True:
        try:
            process_day(subscriber, sub_path)
            print("Day complete. Waiting for next day's data...")
        except Exception as e:
            if "504" in str(e) or "Deadline Exceeded" in str(e):
                time.sleep(10)
            else:
                print(f"Error: {e} — retrying in 10s")
                time.sleep(10)

if __name__ == "__main__":
    main()