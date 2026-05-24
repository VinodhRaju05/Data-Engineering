import json, time, logging, psycopg2
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta, date
from google.cloud import pubsub_v1
from pyproj import Transformer

PROJECT_ID = "cs536-data-engineering"
SUBSCRIPTION_ID = "se_analysis_sub"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = "vinodh"
DB_PASS = "password123"

logging.basicConfig(level=logging.CRITICAL, format="%(asctime)s %(levelname)s %(message)s")

_transformer = Transformer.from_crs("EPSG:2913", "EPSG:4326", always_xy=True)

def state_plane_to_latlon(x, y):
    lon, lat = _transformer.transform(x, y)
    return lat, lon

def get_service_date(record=None):
    if record and record.get("service_date"):
        try:
            return datetime.strptime(record["service_date"], "%Y-%m-%d")
        except:
            pass
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

def seconds_to_timestamp(seconds, base_date):
    if seconds is None:
        return None
    return base_date + timedelta(seconds=int(seconds))

def validate_record(record):
    violations = []
    arrive = record.get("arrive_time")
    if arrive is None or not (0 <= arrive <= 108000):
        violations.append("arrive_time is null or out of range [0, 108000]")
    leave = record.get("leave_time")
    if leave is None:
        violations.append("leave_time is null")
    elif arrive is not None and leave < arrive:
        violations.append("leave_time is less than arrive_time")
    if record.get("vehicle_number") is None:
        violations.append("vehicle_number is null")
    if record.get("trip_number") is None:
        violations.append("trip_number is null")
    route = record.get("route_number")
    if route is None or route <= 0:
        violations.append("route_number is null or <= 0")
    ons = record.get("ons")
    offs = record.get("offs")
    if ons is None or ons < 0:
        violations.append("ons is null or negative")
    if offs is None or offs < 0:
        violations.append("offs is null or negative")
    load = record.get("estimated_load")
    if load is None or load < 0:
        violations.append("estimated_load is null or negative")
    if record.get("location_id") is None:
        violations.append("location_id is null")
    sk = record.get("service_key")
    if sk not in ["W", "S", "U"]:
        violations.append("service_key is not W, S, or U")
    dwell = record.get("dwell")
    if dwell is None or dwell < 0:
        violations.append("dwell is null or negative")
    if violations:
        return False, violations
    return True, []

def transform_record(record):
    base_date = get_service_date(record)
    record.pop("service_date", None)
    record["arrive_time"] = seconds_to_timestamp(record.get("arrive_time"), base_date)
    record["leave_time"]  = seconds_to_timestamp(record.get("leave_time"), base_date)
    record["stop_time"]   = seconds_to_timestamp(record.get("stop_time"), base_date)
    x = record.pop("x_coordinate", None)
    y = record.pop("y_coordinate", None)
    if x is not None and y is not None:
        lat, lon = state_plane_to_latlon(x, y)
        record["GPS_latitude"]  = lat
        record["GPS_longitude"] = lon
    else:
        record["GPS_latitude"]  = None
        record["GPS_longitude"] = None
    return record

def insert_record(cur, r):
    # Use pdx_trip_id if available, otherwise fall back to trip_number
    trip_num = r.get("pdx_trip_id") or r.get("trip_number")
    cur.execute("""
        INSERT INTO stopevent (
            vehicle_number, leave_time, train, route_number, direction,
            service_key, trip_number, stop_time, arrive_time, dwell,
            location_id, door, lift, ons, offs, estimated_load,
            maximum_speed, train_mileage, pattern_distance,
            location_distance, GPS_latitude, GPS_longitude,
            data_source, schedule_status
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        ) ON CONFLICT DO NOTHING
    """, (
        r.get("vehicle_number"), r.get("leave_time"), r.get("train"),
        r.get("route_number"), r.get("direction"), r.get("service_key"),
        trip_num, r.get("stop_time"), r.get("arrive_time"),
        r.get("dwell"), r.get("location_id"), r.get("door"), r.get("lift"),
        r.get("ons"), r.get("offs"), r.get("estimated_load"),
        r.get("maximum_speed"), r.get("train_mileage"), r.get("pattern_distance"),
        r.get("location_distance"), r.get("GPS_latitude"), r.get("GPS_longitude"),
        r.get("data_source"), r.get("schedule_status")
    ))

def process_day(subscriber, sub_path):
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    conn.autocommit = True
    cur = conn.cursor()
    vehicle_ids, trip_ids = set(), set()
    total, stored, errors = 0, 0, 0
    invalid_records = []
    begin_wall = None
    sentinel_found, total_count = False, None
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
            total += 1
            is_valid, violations = validate_record(data)
            if not is_valid:
                errors += 1
                invalid_records.append({"violations": violations, "record": data})
                continue
            record = transform_record(data)
            if record.get("vehicle_number"): vehicle_ids.add(record["vehicle_number"])
            if record.get("trip_number"): trip_ids.add(record["trip_number"])
            try:
                insert_record(cur, record)
                stored += 1
            except Exception as e:
                logging.warning("DB error: %s", e)
        if ack_ids:
            subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
        if sentinel_found and total_count is not None and total >= total_count:
            break
    if invalid_records:
        fname = f"/home/vinodh/se_invalid_{date.today().isoformat()}.json"
        with open(fname, "w") as f:
            json.dump(invalid_records, f, indent=2, default=str)
    end_wall = datetime.now()
    elapsed = (end_wall - begin_wall).total_seconds() if begin_wall else 0
    tp = total / elapsed if elapsed > 0 else 0
    print(f"BEGIN_TIMESTAMP:   {begin_wall.isoformat() if begin_wall else chr(78)+chr(47)+chr(65)}")
    print(f"NUM_VEHICLES:      {len(vehicle_ids)}")
    print(f"NUM_TRIPS:         {len(trip_ids)}")
    print(f"NUM_RECORDS:       {total}")
    print(f"VALIDATION_ERRORS: {errors}")
    print(f"STORED:            {stored}")
    print(f"END_TIMESTAMP:     {end_wall.isoformat()}")
    print(f"WALLTIME:          {elapsed:.2f}s")
    print(f"THROUGHPUT:        {tp:.1f} records/sec")
    cur.close()
    conn.close()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    print("se_analysis.py started, waiting for data...")
    while True:
        try:
            process_day(subscriber, sub_path)
            print("Day complete. Waiting for next day...")
        except Exception as e:
            if "504" in str(e) or "Deadline" in str(e):
                time.sleep(10)
            else:
                print(f"Error: {e} - retrying in 10s")
                time.sleep(10)

if __name__ == "__main__":
    main()