import requests
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "cs536-data-engineering"
TOPIC_ID = "bc_topic"

VEHICLE_IDS = [
    2903, 2905, 2907, 2910, 2914, 2915, 2919, 2920, 2921, 2923,
    2924, 2931, 2933, 2939, 3001, 3007, 3010, 3012, 3014, 3018,
    3020, 3027, 3028, 3037, 3042, 3046, 3054, 3055, 3056, 3059,
    3102, 3107, 3108, 3110, 3112, 3113, 3115, 3118, 3121, 3123,
    3125, 3131, 3137, 3138, 3139, 3142, 3144, 3145, 3146, 3158,
    3162, 3166, 3168, 3170, 3204, 3207, 3208, 3209, 3210, 3214,
    3217, 3219, 3223, 3228, 3230, 3231, 3233, 3239, 3240, 3242,
    3246, 3248, 3249, 3250, 3251, 3252, 3260, 3261, 3265, 3267,
    3301, 3304, 3308, 3312, 3315, 3317, 3322, 3326, 3329, 3330,
    3407, 3408, 3412, 3414, 3418, 3505, 3507, 3513, 3518, 3519,
    3521, 3524, 3528, 3529, 3535, 3536, 3545, 3548, 3551, 3552,
    3554, 3561, 3563, 3566, 3568, 3569, 3572, 3576, 3605, 3606,
    3607, 3610, 3611, 3614, 3620, 3621, 3623, 3628, 3631, 3637,
    3647, 3648, 3649, 3702, 3708, 3715, 3716, 3717, 3718, 3719,
    3721, 3724, 3725, 3728, 3730, 3733, 3737, 3739, 3742, 3743,
    3745, 3751, 3755, 3756, 3902, 3909, 3912, 3913, 3915, 3919,
    3925, 3928, 3932, 3935, 3936, 3939, 3940, 3942, 3943, 3950,
    3959, 3962, 3963, 4003, 4006, 4009, 4010, 4021, 4023, 4025,
    4033, 4036, 4042, 4045, 4046, 4050, 4053, 4055, 4059, 4060,
    4066, 4067, 4069, 4070, 4203, 4211, 4213, 4217, 4218, 4219,
    4221, 4222, 4224, 4234, 4239, 4302, 4303, 4304, 4305, 4502,
    4504, 4505, 4511, 4513, 4516, 4517, 4518, 4520, 4522, 4528,
    4530, 4531,
]

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    begin_ts = datetime.now()
    total_breadcrumbs = 0
    vehicles_with_data = set()
    for vehicle_id in VEHICLE_IDS:
        url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            pass  # skip vehicles with no data
            continue
        if not records:
            continue
        vehicles_with_data.add(vehicle_id)
        futures = []
        for record in records:
            msg = json.dumps(record).encode("utf-8")
            future = publisher.publish(topic_path, msg)
            futures.append(future)
            total_breadcrumbs += 1
        for f in futures:
            f.result()
    time.sleep(5)
    sentinel = json.dumps({"sentinel": True, "total_count": total_breadcrumbs}).encode("utf-8")
    publisher.publish(topic_path, sentinel, sentinel="true").result()
    end_ts = datetime.now()
    elapsed = (end_ts - begin_ts).total_seconds()
    throughput = total_breadcrumbs / elapsed if elapsed > 0 else 0
    print(f"BEGIN_TIMESTAMP: {begin_ts.isoformat()}")
    print(f"NUM_VEHICLES:    {len(vehicles_with_data)}")
    print(f"NUM_BREADCRUMBS: {total_breadcrumbs}")
    print(f"WALLTIME:        {elapsed:.2f}s")
    print(f"THROUGHPUT:      {throughput:.1f} breadcrumbs/sec")
    print(f"END_TIMESTAMP:   {end_ts.isoformat()}")

if __name__ == "__main__":
    main()