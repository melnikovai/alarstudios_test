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
where fsmd.event_time  - fsma.off_block_time >= interval '15' minute
), total_flights_fct as ( -- number of flights from schedule
	select
		event_date,
		count(*) as fct_flights
	from alar.alar_generate_flight
	where movement_type = 'departure'
	group by event_date
), total_flights_schedule as ( -- number of flights from schedule
	select
		day_number,
		count(*) as schedule_flights
	from (
		select
			flight_number,
			day_of_week,
			row_number() over (partition by flight_number order by flight_number) as day_number
		from (
			select
				flight_number,
				s.day_of_week
			from alar.alar_staging_schedule as t, regexp_split_to_table(t.weekly_schedule, '') as s(day_of_week)
		) as q
	) as s
	where day_of_week <> '-'
	group by day_number
)
select
	a.event_date,
	sch.schedule_flights,
	f.fct_flights,
	a.late_flights,
	t.late_depart_flights,
	a.late_flights - t.late_depart_flights as strong_wind_flights,
	l.late_dep_arrive_in_time_flights,
	round(f.fct_flights :: numeric / sch.schedule_flights * 100, 2) as fct_flights_percent,
	round(a.late_flights :: numeric / f.fct_flights * 100, 2) as late_flights_percent,
	round(t.late_depart_flights :: numeric / a.late_flights * 100, 2) as late_depart_flights_percent,
	100 - round(t.late_depart_flights :: numeric / a.late_flights * 100, 2) as strong_wind_percent,
	l.late_dep_arrive_in_time_percent
from (
	select
		event_date,
		count(*) as late_flights
	from alar.fct_flights_with_schedule_v as a
	where movement_type = 'arrival' and a.event_time - a.on_block_time >= interval '15' minute
	group by event_date
) as a
join (
	select
		event_date,
		count(*) as late_depart_flights
	from t
	group by event_date
) as t on a.event_date = t.event_date
join total_flights_fct f on a.event_date = f.event_date
join total_flights_schedule as sch on extract(isodow from a.event_date) = sch.day_number
join alar.alar_late_depart_in_time_arr_flights_v as l on a.event_date = l.event_date
order by a.event_date;