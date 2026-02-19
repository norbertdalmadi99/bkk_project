SELECT pg_database_size('bkk_db');
create schema if not exists bkk;

create table if not exists bkk.stops (
  stop_id text primary key,
  stop_name text,
  stop_lat double precision,
  stop_lon double precision,
  stop_code text,
  location_type text,
  location_sub_type text,
  parent_station text,
  wheelchair_boarding text
);

create table if not exists bkk.agency (
	agency_id text primary key,
	agency_name text,
	agency_url text,
	agency_timezone text,
	agency_lang text,
	agency_phone text
);

create table if not exists bkk.routes(
	route_id text primary key,
	agency_id text,
	route_short_name text,
	route_long_name text,
	route_type text,
	route_desc text,
	route_color text,
	route_text_color text,
	route_sort_order text,
	CONSTRAINT fk_routes_agency
        FOREIGN KEY (agency_id)
        REFERENCES bkk.agency(agency_id)
        ON DELETE CASCADE
);

create table if not exists bkk.shapes(
	shape_id text primary key,
	shape_pt_sequence text,
	shape_pt_lat double precision,
	shape_pt_lon double precision,
	shape_dist_traveled text
);

create table if not exists bkk.calendar_dates(
	service_id text primary key,
	dates text,
	exception_type text
);

create table if not exists bkk.trips(
	route_id text,
	trip_id text primary key,
	service_id text,
	trip_headsign text,
	direction_id text,
	block_id text,
	shape_id text,
	wheelchair_accessible text,
	bikes_allowed text,
	CONSTRAINT fk_trips_route
        FOREIGN KEY (route_id)
        REFERENCES bkk.routes(route_id)
        ON DELETE CASCADE,
	CONSTRAINT fk_trips_shape
        FOREIGN KEY (shape_id)
        REFERENCES bkk.shapes(shape_id)
        ON DELETE CASCADE,
	CONSTRAINT fk_trips_service
        FOREIGN KEY (service_id)
        REFERENCES bkk.calendar_dates(service_id)
        ON DELETE CASCADE
);

create table if not exists bkk.translations(
	table_name text,
	field_name text,
	language text,
	translation text,
	field_value text
);

create table if not exists bkk.pathways(
	pathway_id text primary key,
	pathway_mode text,
	is_bidirectional text,
	from_stop_id text,
	to_stop_id text,
	traversal_time text
);


create table if not exists bkk.stop_times(
	trip_id text,
	stop_id text,
	arrival_time text,
	departure_time text,
	stop_sequence text,
	stop_headsign text,
	pickup_type text,
	drop_off_type text,
	shape_dist_traveled text,
	CONSTRAINT fk_stop_times_trip
        FOREIGN KEY (trip_id)
        REFERENCES bkk.trips(trip_id)
        ON DELETE CASCADE,
	CONSTRAINT fk_stop_times_stop
        FOREIGN KEY (stop_id)
        REFERENCES bkk.stops(stop_id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS stop_times_trip_seq_uq
ON bkk.stop_times (trip_id, stop_sequence);

SELECT * FROM bkk.stops;
SELECT * FROM bkk.agency;
SELECT * FROM bkk.routes;
SELECT * FROM bkk.shapes;
SELECT * FROM bkk.calendar_dates;
SELECT * FROM bkk.trips;
SELECT * FROM bkk.pathways;
SELECT * FROM bkk.stop_times;





create table if not exists bkk.vehicle_positions_history (
  id bigserial primary key,
  fetched_at timestamptz not null default now(),
  vehicle_id text,
  vehicle_label text,
  trip_id text,
  route_id text,
  ts timestamptz,
  lat double precision,
  lon double precision,
  stop_id text,
  license_plate text

);

create index if not exists idx_vph_fetched_at on bkk.vehicle_positions_history (fetched_at desc);
create index if not exists idx_vph_vehicle_id on bkk.vehicle_positions_history (vehicle_id);
create index if not exists idx_vph_route_id on bkk.vehicle_positions_history (route_id);

SELECT * FROM bkk.vehicle_positions_history;




