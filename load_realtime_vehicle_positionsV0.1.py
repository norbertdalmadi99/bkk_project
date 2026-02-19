import os
import requests
import psycopg
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""Global variables for more info check env variables"""
DB = os.environ["DATABASE_URL"]
API_KEY = os.environ["BKK_API_KEY"]
VEH_URL = os.environ["BKK_GTFSRT_VEHICLE_POS_URL"]

def fetch_feed_bytes() -> bytes:
    """Making connection with the BKK API via URL and saves the info"""
    r = requests.get(
        VEH_URL,
        timeout=30,
        headers={"X-API-KEY": API_KEY},
        params={"key": API_KEY},
        verify=False
    )
    r.raise_for_status()
    return r.content

def parse_and_insert(feed_bytes: bytes):
    """Converting the API information into SQL related data then making the connection"""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_bytes)

    rows = []
    fetched_at = datetime.now(timezone.utc)

    for ent in feed.entity:
        """If the entity don't have a field called vehicle it's skips the whole SQL insert procedure"""
        if not ent.HasField("vehicle"):
            continue

        """Defining the columns for the SQL query"""
        v = ent.vehicle

        vehicle_id = (v.vehicle.id if v.HasField("vehicle") else None) or ent.id
        vehicle_label = (v.vehicle.label if v.HasField("vehicle") else None) or ent.id
        trip_id = v.trip.trip_id if v.HasField("trip") else None
        route_id = v.trip.route_id if v.HasField("trip") else None

        ts = datetime.fromtimestamp(v.timestamp, tz=timezone.utc) if v.timestamp else None

        lat = v.position.latitude if v.HasField("position") else None
        lon = v.position.longitude if v.HasField("position") else None

        stop_id = v.stop_id if v.HasField("stop_id") else None
        license_plate = v.vehicle.license_plate if v.HasField("vehicle") else None

        rows.append((fetched_at, vehicle_id, vehicle_label, trip_id, route_id, ts, lat, lon, stop_id, license_plate))

    sql = """
    insert into bkk.vehicle_positions_history
      (fetched_at, vehicle_id, vehicle_label, trip_id, route_id, ts, lat, lon, stop_id, license_plate)
    values
      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    """Making the connection with the SQL database"""
    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()

    return len(rows)

if __name__ == "__main__":
    feed_bytes = fetch_feed_bytes()
    n = parse_and_insert(feed_bytes)
    print(f"OK: {n} position inserted")
