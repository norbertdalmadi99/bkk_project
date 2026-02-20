import os, io, zipfile, csv
import requests
import psycopg
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB = os.environ["DATABASE_URL"]
GTFS_URL = os.environ["BKK_GTFS_STATIC_URL"]

def fetch_zip() -> bytes:
    """Reading the ZIP file from the specified URL"""
    r = requests.get(GTFS_URL, timeout=20, verify=False)
    r.raise_for_status()
    return r.content

def read_stops_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the stops.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("stops.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["stop_id"],
                row.get("stop_name"),
                float(row["stop_lat"]) if row.get("stop_lat") else None,
                float(row["stop_lon"]) if row.get("stop_lon") else None,
                row.get("stop_code"),
                row.get("location_type"),
                row.get("location_sub_type"),
                row.get("parent_station"),
                row.get("wheelchair_boarding"),
            )

def read_agency_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the agency.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("agency.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["agency_id"],
                row.get("agency_name"),
                row.get("agency_url"),
                row.get("agency_timezone"),
                row.get("agency_lang"),
                row.get("agency_phone"),
            )

def read_routes_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the routes.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("routes.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["route_id"],
                row.get("agency_id"),
                row.get("route_short_name"),
                row.get("route_long_name"),
                row.get("route_type"),
                row.get("route_desc"),
                row.get("route_color"),
                row.get("route_text_color"),
                row.get("route_sort_order"),
            )

def read_shapes_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the shapes.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("shapes.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["shape_id"],
                row.get("shape_pt_sequence"),
                float(row["shape_pt_lat"]) if row.get("shape_pt_lat") else None,
                float(row["shape_pt_lon"]) if row.get("shape_pt_lon") else None,
                row.get("shape_dist_traveled"),
            )

def read_calendar_dates_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the calendar_dates.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("calendar_dates.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["service_id"],
                row.get("dates"),
                row.get("exception_type"),
            )

def read_trips_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the trips.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("trips.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row.get("route_id"),
                row["trip_id"],
                row.get("service_id"),
                row.get("trip_headsign"),
                row.get("direction_id"),
                row.get("block_id"),
                row.get("shape_id"),
                row.get("wheelchair_accessible"),
                row.get("bikes_allowed"),
            )

def read_pathways_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the pathways.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("pathways.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row["pathway_id"],
                row.get("pathway_mode"),
                row.get("is_bidirectional"),
                row.get("from_stop_id"),
                row.get("to_stop_id"),
                row.get("traversal_time"),
            )

def read_stop_times_from_zip(gtfs_zip_bytes: bytes):
    """Uses the ZIP and reading the stop_times.txt from it then specifies the columns"""
    z = zipfile.ZipFile(io.BytesIO(gtfs_zip_bytes))
    with z.open("stop_times.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            yield (
                row.get("trip_id"),
                row.get("stop_id"),
                row.get("arrival_time"),
                row.get("departure_time"),
                row.get("stop_sequence"),
                row.get("stop_headsign"),
                row.get("pickup_type"),
                row.get("drop_off_type"),
                row.get("shape_dist_traveled"),
            )

def upsert(stops_rows, agency_rows, routes_rows, shapes_rows, calendar_dates_rows, trips_rows, pathways_rows, stop_times_rows):
    """Specifies the SQL Query for every table one by one then making the connection"""
    sql_stops = """
    insert into bkk.stops (stop_id, stop_name, stop_lat, stop_lon, stop_code, location_type, location_sub_type, parent_station, wheelchair_boarding)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    on conflict (stop_id) do update set
      stop_name = excluded.stop_name,
      stop_lat  = excluded.stop_lat,
      stop_lon  = excluded.stop_lon,
      stop_code  = excluded.stop_code,
      location_type  = excluded.location_type,
      location_sub_type  = excluded.location_sub_type,
      parent_station  = excluded.parent_station,
      wheelchair_boarding = excluded.wheelchair_boarding;
    """

    sql_agency = """
    insert into bkk.agency (agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone)
    values (%s,%s,%s,%s,%s,%s)
    on conflict (agency_id) do update set
      agency_name = excluded.agency_name,
      agency_url  = excluded.agency_url,
      agency_timezone  = excluded.agency_timezone,
      agency_lang  = excluded.agency_lang,
      agency_phone  = excluded.agency_phone;
    """

    sql_routes = """
    insert into bkk.routes (route_id, agency_id, route_short_name, route_long_name, route_type, route_desc, route_color, route_text_color, route_sort_order)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    on conflict (route_id) do update set
      agency_id = excluded.agency_id,
      route_short_name  = excluded.route_short_name,
      route_long_name  = excluded.route_long_name,
      route_type  = excluded.route_type,
      route_desc  = excluded.route_desc,
      route_color  = excluded.route_color,
      route_text_color  = excluded.route_text_color,
      route_sort_order  = excluded.route_sort_order;
    """

    sql_shapes = """
    insert into bkk.shapes (shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon, shape_dist_traveled)
    values (%s,%s,%s,%s,%s)
    on conflict (shape_id) do update set
      shape_pt_sequence = excluded.shape_pt_sequence,
      shape_pt_lat  = excluded.shape_pt_lat,
      shape_pt_lon  = excluded.shape_pt_lon,
      shape_dist_traveled  = excluded.shape_dist_traveled;
    """

    sql_calendar_dates = """
    insert into bkk.calendar_dates (service_id, dates, exception_type)
    values (%s,%s,%s)
    on conflict (service_id) do update set
      dates = excluded.dates,
      exception_type  = excluded.exception_type;
    """

    sql_trips = """
    insert into bkk.trips (route_id, trip_id, service_id, trip_headsign, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    on conflict (trip_id) do update set
      route_id = excluded.route_id,
      service_id  = excluded.service_id,
      trip_headsign  = excluded.trip_headsign,
      direction_id  = excluded.direction_id,
      block_id  = excluded.block_id,
      shape_id  = excluded.shape_id,
      wheelchair_accessible  = excluded.wheelchair_accessible,
      bikes_allowed  = excluded.bikes_allowed;
    """

    sql_pathways = """
    insert into bkk.pathways (pathway_id, pathway_mode, is_bidirectional, from_stop_id, to_stop_id, traversal_time)
    values (%s,%s,%s,%s,%s,%s)
    on conflict (pathway_id) do update set
      pathway_mode = excluded.pathway_mode,
      is_bidirectional  = excluded.is_bidirectional,
      from_stop_id  = excluded.from_stop_id,
      to_stop_id  = excluded.to_stop_id,
      traversal_time  = excluded.traversal_time;
    """

    sql_stop_times = """
    insert into bkk.stop_times (trip_id, stop_id, arrival_time, departure_time, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    on conflict (trip_id, stop_sequence) do update set
      stop_id  = excluded.stop_id,
      arrival_time  = excluded.arrival_time,
      departure_time  = excluded.departure_time,
      stop_headsign  = excluded.stop_headsign,
      pickup_type  = excluded.pickup_type,
      drop_off_type  = excluded.drop_off_type,
      shape_dist_traveled  = excluded.shape_dist_traveled;
    """

    with psycopg.connect(DB) as conn:
        with conn.cursor() as cur:
            """Execute the queries using the specified string and info from zip"""
            cur.executemany(sql_stops, list(stops_rows))
            cur.executemany(sql_agency, list(agency_rows))
            cur.executemany(sql_routes, list(routes_rows))
            cur.executemany(sql_shapes, list(shapes_rows))
            cur.executemany(sql_calendar_dates, list(calendar_dates_rows))
            cur.executemany(sql_trips, list(trips_rows))
            cur.executemany(sql_pathways, list(pathways_rows))
            cur.executemany(sql_stop_times, list(stop_times_rows))
        conn.commit()


if __name__ == "__main__":
    data = fetch_zip()
    print("ZIP loaded")
    stops = read_stops_from_zip(data)
    print("STOPS: loaded")
    agency = read_agency_from_zip(data)
    print("AGENCY: loaded")
    routes = read_routes_from_zip(data)
    print("ROUTES: loaded")
    shapes = read_shapes_from_zip(data)
    print("SHAPES: loaded")
    calendar_dates = read_calendar_dates_from_zip(data)
    print("CALENDAR_DATES: loaded")
    trips = read_trips_from_zip(data)
    print("TRIPS: loaded")
    pathways = read_pathways_from_zip(data)
    print("PATHWAYS: loaded")
    stop_times = read_stop_times_from_zip(data)
    print("STOP_TIMES: loaded")
    upsert(stops,agency, routes, shapes, calendar_dates, trips, pathways, stop_times)
    print("SQL Insert OK")