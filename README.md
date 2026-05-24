# CS536 Data Engineering Project

## Project Overview

This project builds a real-time data pipeline that collects, processes, stores, and visualizes GPS sensor data from TriMet Portland's public transit system. TriMet buses emit two types of data: BreadCrumb records (GPS location and speed) and StopEvent records (passenger boardings and alightings at each stop).

---

## System Architecture

The pipeline runs across 6 Google Cloud VMs in 3 regions:

| VM | Region | Role |
|---|---|---|
| publisher-vm / publisher-test | us-west1 (Oregon) | Fetches data from TriMet API and publishes to Pub/Sub |
| analysis-vm / analysis-test | europe-west1 (Belgium) | Subscribes, validates, transforms, and stores in PostgreSQL |
| backup-vm / backup-test | asia-east1 (Taiwan) | Subscribes and writes compressed backup files |

**Google Cloud Pub/Sub Topics:**
- `bc_topic` — BreadCrumb messages
- `se_topic` — StopEvent messages

**Database:** PostgreSQL on analysis-vm  
- Database: `breadcrumbs`  
- Tables: `breadcrumb`, `stopevent`

---

## Repository Structure

```
Data-Engineering/
├── part1/
│   ├── publisher.py      # Fetches BreadCrumb data and publishes to Pub/Sub
│   ├── analysis.py       # Subscribes and stores BreadCrumb data
│   └── backup.py         # Subscribes and backs up BreadCrumb data
├── part2/
│   ├── publisher.py      # Updated publisher with sentinel
│   ├── analysis.py       # Added validation, transformation, PostgreSQL storage
│   └── backup.py         # Compressed backup with gzip
└── part3/
    ├── publisher.py      # BreadCrumb publisher (final version)
    ├── se_publisher.py   # StopEvent publisher (HTML parsing, PDX_TRIP ID)
    ├── analysis.py       # BreadCrumb analysis pipeline (final version)
    ├── se_analysis.py    # StopEvent analysis pipeline with coordinate conversion
    ├── backup.py         # BreadCrumb backup pipeline (final version)
    └── se_backup.py      # StopEvent backup pipeline
```

---

## Part 1 — Basic Pipeline

Built three programs:
- `publisher.py` — fetches BreadCrumb JSON from TriMet API for 222 vehicles and publishes to `bc_topic`
- `analysis.py` — subscribes to `analysis_sub` and prints received data
- `backup.py` — subscribes to `backup_sub` and writes backup files

---

## Part 2 — Validation, Transformation, and Storage

Extended the pipeline with:

**Validations (6 assertions):**
1. GPS_LATITUDE must be in [-90, 90]
2. GPS_LONGITUDE must be in [-180, 180]
3. VEHICLE_ID must not be null
4. OPD_DATE must be valid format
5. ACT_TIME must be in [0, 86400]
6. EVENT_NO_TRIP and EVENT_NO_STOP must not be null

**Transformations:**
- Convert OPD_DATE + ACT_TIME to proper timestamp
- Calculate speed from distance/time between consecutive breadcrumbs
- Rename fields: EVENT_NO_TRIP → trip_id, VEHICLE_ID → vehicle_id, etc.

**BreadCrumb table schema:**
```sql
CREATE TABLE breadcrumb (
    trip_id    INTEGER,
    vehicle_id INTEGER,
    timestamp  TIMESTAMP,
    latitude   DOUBLE PRECISION,
    longitude  DOUBLE PRECISION,
    speed      DOUBLE PRECISION,
    meters     DOUBLE PRECISION
);
```

---

## Part 3 — StopEvent Pipeline

Added StopEvent data pipeline:

**se_publisher.py:**
- Fetches HTML from TriMet StopEvent API
- Parses HTML with BeautifulSoup
- Extracts service date from `<h1>` tag
- Extracts PDX_TRIP ID from `<h2>` tags to match BreadCrumb trip_id
- Publishes to `se_topic`

**se_analysis.py:**
- 10 validations including arrive_time, leave_time, ons, offs, service_key
- Converts Oregon State Plane coordinates (EPSG:2913) to GPS lat/lon using pyproj
- Converts time fields from seconds-past-midnight to timestamps
- Stores in PostgreSQL stopevent table

**StopEvent table schema:**
```sql
CREATE TABLE stopevent (
    vehicle_number    INTEGER NOT NULL,
    leave_time        TIMESTAMP,
    train             INTEGER,
    route_number      INTEGER,
    direction         SMALLINT,
    service_key       CHAR(1),
    trip_number       INTEGER NOT NULL,
    stop_time         TIMESTAMP,
    arrive_time       TIMESTAMP NOT NULL,
    dwell             INTEGER,
    location_id       INTEGER,
    door              INTEGER,
    lift              INTEGER,
    ons               INTEGER,
    offs              INTEGER,
    estimated_load    INTEGER,
    maximum_speed     INTEGER,
    train_mileage     DOUBLE PRECISION,
    pattern_distance  DOUBLE PRECISION,
    location_distance DOUBLE PRECISION,
    GPS_latitude      DOUBLE PRECISION,
    GPS_longitude     DOUBLE PRECISION,
    data_source       SMALLINT,
    schedule_status   SMALLINT,
    PRIMARY KEY (vehicle_number, trip_number, arrive_time)
);
```

**Pipeline Run Statistics (3 days):**

| Pipeline Date | TriMet Date | Breadcrumbs | StopEvents | BC Stored | SE Stored | Vehicles |
|---|---|---|---|---|---|---|
| 2026-05-21 | 2023-01-22 | 515,711 | 64,622 | 503,172 | 63,334 | 123 |
| 2026-05-23 | 2023-01-24 | 734,797 | 90,965 | 720,790 | 90,965 | 153 |
| 2026-05-24 | 2023-01-25 | 763,692 | 95,632 | 743,877 | 95,632 | 158 |

---

## Part 4 — Final Presentation Video

A 10-minute video summarizing the project including:
- System architecture diagram
- Data pipeline description
- Visualizations with SQL queries
- Challenges and lessons learned

---

## Vehicle Group

**Team Shazam** — 222 vehicles (IDs 2903–4531)

---

## Technologies Used

- Python 3
- Google Cloud Pub/Sub
- Google Cloud Compute Engine
- PostgreSQL
- BeautifulSoup (HTML parsing)
- pyproj (coordinate conversion)
- Folium (interactive maps)
- Matplotlib / Seaborn (charts)
- Systemd (service management)
