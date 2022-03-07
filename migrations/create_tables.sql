CREATE SCHEMA IF NOT EXISTS alar;

DROP TABLE IF EXISTS alar.airports_dict;
DROP INDEX IF EXISTS alar_airports_dict_idx;

CREATE TABLE IF NOT EXISTS alar.airports_dict (
    id SERIAL PRIMARY KEY,
    airport_id varchar(16),
    airport_type varchar(32),
    airport_name varchar(256),
    elevation_ft integer,
    continent varchar(4),
    country varchar(4),
    region varchar(16),
    municipality varchar(128),
    gps_code varchar(8),
    iata varchar(32),
    local_code varchar(8),
    coordinates point
);

CREATE TABLE IF NOT EXISTS alar.alar_api_airports (
	id SERIAL PRIMARY KEY,
	airport_id varchar(16),
	city varchar(256),
	country varchar(128),
	latitude double precision,
	longitude double precision
);

CREATE TABLE IF NOT EXISTS alar.alar_staging_schedule (
	flight_number integer PRIMARY KEY,
	origin_airport_code varchar(4),
	destination_airport_code varchar(4),
	aircraft_type varchar(4),
	weekly_schedule varchar(8),
	off_block_time varchar(8),
	on_block_time varchar(8)
);

CREATE TABLE IF NOT EXISTS alar.alar_generate_flight (
	flight_number integer,
	movement_type varchar(16),
	event_date date,
	event_time time
);

CREATE INDEX alar_airports_dict_idx ON alar.airports_dict (airport_id);

COPY alar.airports_dict (airport_id, airport_type, airport_name, elevation_ft, continent, country, region, municipality, gps_code, iata, local_code, coordinates) FROM '/var/lib/postgresql/pg_data/airport_codes.csv' CSV HEADER;