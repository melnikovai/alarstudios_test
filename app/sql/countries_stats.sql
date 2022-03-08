with total_flights_fct as ( -- number of flights from fact table
	select
		flight_number,
		count(*) as fct_flights
	from alar.alar_generate_flight
	where movement_type = 'departure'
	group by flight_number
), late_flights as (
	select
		flight_number,
		count(*) as late_flights_total
	from alar.fct_flights_with_schedule_v as a
	where movement_type = 'arrival' and a.event_time - a.on_block_time >= interval '15' minute
	group by flight_number
), t as (
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
	where movement_type = 'arrival' and event_time - on_block_time >= interval '15' minute
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
	where movement_type = 'departure'
  	  and event_time - off_block_time >= interval '15' minute
) as fsmd
 on fsma.event_date = fsmd.event_date
and fsma.flight_number = fsmd.flight_number
), t30 as (
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
), flight_number_late_stat as (
	select
		a.flight_number,
		a.origin_airport_code,
		a.aircraft_type,
		tff.fct_flights,
		lf.late_flights_total,
		b.late_depart,
		f.late_dep_arr_in_time_cnt
	from alar.alar_staging_schedule a
	left join (
		select
			flight_number,
			count(*) as late_depart
		from t
		group by flight_number
		order by flight_number
	) as b on a.flight_number = b.flight_number
	left join (
		select
			flight_number,
			count(*) as late_dep_arr_in_time_cnt
		from t30
		group by flight_number
		order by flight_number
	) as f on a.flight_number = f.flight_number
	left join total_flights_fct tff on a.flight_number = tff.flight_number
	left join late_flights lf on a.flight_number = lf.flight_number
), airport_late_stat as (
	select
		c.id,
		c.airport_id,
		c.city,
		c.country,
		d.total_fct_flights,
		d.late_flights_total,
		d.late_depart,
		d.late_dep_arr_in_time_cnt
	from alar.alar_api_airports c
	left join (
		select
			origin_airport_code,
			sum(late_flights_total) as late_flights_total,
			sum(late_depart) as late_depart,
			sum(fct_flights) as total_fct_flights,
			sum(late_dep_arr_in_time_cnt) as late_dep_arr_in_time_cnt
		from flight_number_late_stat
		group by origin_airport_code
	) as d on c.airport_id = d.origin_airport_code
)
select
	country,
	total_fct_flights,
	late_flights_total,
	late_depart,
	late_flights_total - late_depart as late_strong_wind,
	late_dep_arr_in_time_cnt,
	round(late_flights_total / total_fct_flights * 100, 2) as late_flights_total_percent,
	round(late_depart / total_fct_flights * 100, 2) as late_percent,
	round((late_flights_total - late_depart) / total_fct_flights * 100, 2) as late_strong_wind_percent,
	round(late_dep_arr_in_time_cnt / total_fct_flights * 100, 2) as late_dep_arr_in_time_percent
from (
	select
		country,
		sum(total_fct_flights) as total_fct_flights,
		sum(late_flights_total) as late_flights_total,
		sum(late_depart) as late_depart,
		sum(late_dep_arr_in_time_cnt) as late_dep_arr_in_time_cnt
	from airport_late_stat
	group by country
) as e
order by late_depart desc nulls last;
