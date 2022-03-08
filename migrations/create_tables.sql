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
	off_block_time time,
	on_block_time time
);

CREATE TABLE IF NOT EXISTS alar.alar_generate_flight (
	flight_number integer,
	movement_type varchar(16),
	event_date date,
	event_time time
);

CREATE INDEX alar_airports_dict_idx ON alar.airports_dict (airport_id);

create or replace view alar.fct_flights_with_schedule_v as
with schedule_by_day_number as ( -- number of flights by day number of week from schedule
	select
		flight_number,
		off_block_time,
		on_block_time,
		day_of_week,
		dow
	from (
		select
			flight_number,
			off_block_time,
			on_block_time,
			s.day_of_week,
			row_number() over (partition by flight_number) as dow
		from alar.alar_staging_schedule as t, regexp_split_to_table(t.weekly_schedule, '') as s(day_of_week)
	) as q
	where day_of_week <> '-'
)
select
	agf.event_date,
	agf.flight_number,
	agf.movement_type,
	agf.event_time,
	sdn.off_block_time,
	sdn.on_block_time,
	agf.dow
from (
	select
		flight_number,
		event_date,
		movement_type,
		event_time,
		extract(isodow from event_date) as dow
	from alar.alar_generate_flight
) as agf
join schedule_by_day_number sdn on
	agf.flight_number = sdn.flight_number
and agf.dow = sdn.dow;

create or replace view alar.alar_late_depart_in_time_arr_flights_v as
with t as (
select
	fsma.event_date,
	fsma.flight_number,
	fsma.movement_type as movement_type_1,
	fsma.event_time,
	fsma.off_block_time,
	fsma.on_block_time,
	fsmd.movement_type as movement_type_2,
	fsmd.event_time as departure_time
from (
	select
		event_date,
		flight_number,
		movement_type,
		event_time,
		off_block_time,
		on_block_time
	from alar.fct_flights_with_schedule_v
	where movement_type = 'arrival' and event_time - on_block_time <= interval '0' minute
) as fsma
join (
	select
		event_date,
		flight_number,
		movement_type,
		event_time,
		off_block_time,
		on_block_time
	from alar.fct_flights_with_schedule_v
	where movement_type = 'departure' and event_time - off_block_time >= interval '15' minute
) as fsmd
 on fsma.event_date = fsmd.event_date
and fsma.flight_number = fsmd.flight_number
), total_flights_fct as ( -- number of flights from schedule
	select
		event_date,
		count(*) as fct_flights
	from alar.alar_generate_flight
	where movement_type = 'departure'
	group by event_date
)
select
	lat.event_date,
	tfs.fct_flights,
	lat.late_dep_arrive_in_time_flights,
	round(lat.late_dep_arrive_in_time_flights :: numeric / tfs.fct_flights * 100, 2) as late_dep_arrive_in_time_percent
from (
	select
		event_date,
		count(*) as late_dep_arrive_in_time_flights
	from t
	group by event_date
) as lat
join total_flights_fct as tfs on lat.event_date = tfs.event_date
order by event_date;

COPY alar.airports_dict (airport_id, airport_type, airport_name, elevation_ft, continent, country, region, municipality, gps_code, iata, local_code, coordinates) FROM '/var/lib/postgresql/pg_data/airport_codes.csv' CSV HEADER;