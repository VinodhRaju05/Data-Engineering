import json, time, logging, psycopg2
from datetime import datetime, timedelta, date
from google.cloud import pubsub_v1

PROJECT_ID = "cs536-data-engineering"
SUBSCRIPTION_ID = "analysis_sub"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = "vinodh"
DB_PASS = "password123"

logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s %(levelname)s %(message)s")

def validate_record(record):
    violations = []
    lat = record.get("GPS_LATITUDE")
    if lat is None or not (-90 <= lat <= 90):
        violations.append("GPS_LATITUDE is null or out of range [-90, 90]")
    lon = record.get("GPS_LONGITUDE")
    if lon is None or not (-180 <= lon <= 180):
        violations.append("GPS_LONGITUDE is null or out of range [-180, 180]")
    if record.get("VEHICLE_ID") is None:
        violations.append("VEHICLE_ID is null")
    opd = record.get("OPD_DATE")
    try:
        if opd is None: raise ValueError()
        datetime.strptime(opd.strip(), "%d%b%Y:%H:%M:%S")
    except:
        violations.append("OPD_DATE is null or invalid format")
    act = record.get("ACT_TIME")
    if act is None or not (0 <= act <= 86400):
        violations.append("ACT_TIME is null or out of range [0, 86400]")
    if record.get("EVENT_NO_TRIP") is None or record.get("EVENT_NO_STOP") is None:
        violations.append("EVENT_NO_TRIP or EVENT_NO_STOP is null")
    if violations:
        for msg in violations:
            pass  # violations stored in invalid_records list
        return False, violations
    return True, []

def transform_record(record, trip_last):
    for f in ["EVENT_NO_STOP","GPS_SATELLITES","GPS_HDOP"]:
        record.pop(f, None)
    base = datetime.strptime(record.pop("OPD_DATE").strip(), "%d%b%Y:%H:%M:%S")
    act = int(record.pop("ACT_TIME"))
    record["timestamp"] = base + timedelta(seconds=act)
    tid = record.get("EVENT_NO_TRIP")
    prev = trip_last.get(tid)
    if prev is None:
        record["speed"] = 0.0
    else:
        dm = record["METERS"] - prev["METERS"]
        dt = (record["timestamp"] - prev["timestamp"]).total_seconds()
        record["speed"] = round(dm/dt, 4) if dt > 0 else 0.0
    trip_last[tid] = {"METERS": record["METERS"], "timestamp": record["timestamp"]}
    record["trip_id"]    = record.pop("EVENT_NO_TRIP")
    record["vehicle_id"] = record.pop("VEHICLE_ID")
    record["longitude"]  = record.pop("GPS_LONGITUDE")
    record["latitude"]   = record.pop("GPS_LATITUDE")
    record["meters"]     = record.pop("METERS")
    return record

def insert_record(cur, r):
    cur.execute(
        "INSERT INTO breadcrumb(trip_id,vehicle_id,timestamp,latitude,longitude,speed,meters) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        (r["trip_id"],r["vehicle_id"],r["timestamp"],r["latitude"],r["longitude"],r["speed"],r["meters"])
    )

def process_day(subscriber, sub_path):
    conn = psycopg2.connect(host=DB_HOST,port=DB_PORT,dbname=DB_NAME,user=DB_USER,password=DB_PASS)
    conn.autocommit = True
    cur = conn.cursor()
    vehicle_ids,trip_ids,trip_last = set(),set(),{}
    total,stored,errors = 0,0,0
    invalid_records = []
    min_ts,max_ts,begin_wall = None,None,None
    sentinel_found,total_count = False,None
    while True:
        try:
            response = subscriber.pull(request={"subscription":sub_path,"max_messages":500},timeout=10)
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
            total += 1
            is_valid, violations = validate_record(data)
            if not is_valid:
                errors += 1
                invalid_records.append({"violations":violations,"record":data})
                continue
            record = transform_record(data, trip_last)
            vid,tid,ts = record.get("vehicle_id"),record.get("trip_id"),record.get("timestamp")
            if vid: vehicle_ids.add(vid)
            if tid: trip_ids.add(tid)
            if ts:
                if min_ts is None or ts < min_ts: min_ts = ts
                if max_ts is None or ts > max_ts: max_ts = ts
            try:
                insert_record(cur, record)
                stored += 1
            except Exception as e:
                logging.warning("DB error: %s", e)
        if ack_ids:
            subscriber.acknowledge(request={"subscription":sub_path,"ack_ids":ack_ids})
        if sentinel_found and total_count is not None and total >= total_count:
            break
    if invalid_records:
        fname = f"/home/vinodh/invalid_data_{date.today().isoformat()}.json"
        with open(fname,"w") as f:
            json.dump(invalid_records,f,indent=2,default=str)
        logging.info("Wrote %d invalid records to %s", len(invalid_records), fname)
    end_wall = datetime.now()
    elapsed = (end_wall - begin_wall).total_seconds() if begin_wall else 0
    tp = total/elapsed if elapsed > 0 else 0
    print(f"BEGIN_TIMESTAMP:   {begin_wall.isoformat() if begin_wall else 'N/A'}")
    print(f"NUM_VEHICLES:      {len(vehicle_ids)}")
    print(f"MIN_BC_TIMESTAMP:  {min_ts}")
    print(f"MAX_BC_TIMESTAMP:  {max_ts}")
    print(f"NUM_TRIPS:         {len(trip_ids)}")
    print(f"NUM_BREADCRUMBS:   {total}")
    print(f"VALIDATION_ERRORS: {errors}")
    print(f"STORED:            {stored}")
    print(f"END_TIMESTAMP:     {end_wall.isoformat()}")
    print(f"WALLTIME:          {elapsed:.2f}s")
    print(f"THROUGHPUT:        {tp:.1f} breadcrumbs/sec")
    cur.close()
    conn.close()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    print("analysis.py started, waiting for data...")
    while True:
        try:
            process_day(subscriber, sub_path)
            print("Day complete. Waiting for next day's data...")
        except Exception as e:
            if "504" in str(e) or "Deadline" in str(e):
                time.sleep(10)
            else:
                print(f"Error: {e} - retrying in 10s")
                time.sleep(10)

if __name__ == "__main__":
    main()