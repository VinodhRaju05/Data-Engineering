import json
import time
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

PROJECT_ID = "cs536-data-engineering"
SUBSCRIPTION_ID = "analysis_sub"

DRAIN_SECONDS = 30

def process_day(subscriber, sub_path):
    vehicle_ids = set()
    trip_ids = set()
    total = 0
    min_ts = None
    max_ts = None
    begin_wall = None
    sentinel_wall = None

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
                if sentinel_wall is None:
                    sentinel_wall = datetime.now()
                sentinel_found = True
                continue

            if begin_wall is None:
                begin_wall = datetime.now()

            vid = data.get("VEHICLE_ID")
            trip = data.get("EVENT_NO_TRIP")
            opd_date = data.get("OPD_DATE", "")
            act_time = data.get("ACT_TIME", 0)

            if vid is not None:
                vehicle_ids.add(vid)
            if trip is not None:
                trip_ids.add(trip)

            try:
                base = datetime.strptime(opd_date.strip(), "%d%b%Y:%H:%M:%S")
                bc_ts = base + timedelta(seconds=int(act_time))
                if min_ts is None or bc_ts < min_ts:
                    min_ts = bc_ts
                if max_ts is None or bc_ts > max_ts:
                    max_ts = bc_ts
            except Exception:
                pass

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
                        trip = data.get("EVENT_NO_TRIP")
                        opd_date = data.get("OPD_DATE", "")
                        act_time = data.get("ACT_TIME", 0)
                        if vid is not None:
                            vehicle_ids.add(vid)
                        if trip is not None:
                            trip_ids.add(trip)
                        try:
                            base = datetime.strptime(opd_date.strip(), "%d%b%Y:%H:%M:%S")
                            bc_ts = base + timedelta(seconds=int(act_time))
                            if min_ts is None or bc_ts < min_ts:
                                min_ts = bc_ts
                            if max_ts is None or bc_ts > max_ts:
                                max_ts = bc_ts
                        except Exception:
                            pass
                        total += 1
                    if ack_ids:
                        subscriber.acknowledge(
                            request={"subscription": sub_path, "ack_ids": ack_ids}
                        )
                except Exception:
                    break

            # WALLTIME uses sentinel timestamp not after drain
            elapsed = (sentinel_wall - begin_wall).total_seconds() if begin_wall else 0
            throughput = total / elapsed if elapsed > 0 else 0

            print(f"BEGIN_TIMESTAMP:  {begin_wall.isoformat() if begin_wall else 'N/A'}")
            print(f"NUM_VEHICLES:     {len(vehicle_ids)}")
            print(f"MIN_BC_TIMESTAMP: {min_ts}")
            print(f"MAX_BC_TIMESTAMP: {max_ts}")
            print(f"NUM_TRIPS:        {len(trip_ids)}")
            print(f"NUM_BREADCRUMBS:  {total}")
            print(f"END_TIMESTAMP:    {sentinel_wall.isoformat()}")
            print(f"WALLTIME:         {elapsed:.2f}s")
            print(f"THROUGHPUT:       {throughput:.1f} breadcrumbs/sec")
            return

def main():
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print("analysis.py started, waiting for data...")
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