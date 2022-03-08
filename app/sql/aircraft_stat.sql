with flight_late_summury as (
	select
		flight_number,
		sum(event_time - on_block_time) as late_sum_min
	from (
		select
			flight_number,
			movement_type,
			event_time,
			on_block_time
		from alar.fct_flights_with_schedule_v
		where movement_type = 'arrival' and event_time - on_block_time >= interval '15' minute
	) as a
	group by flight_number
), airport_late_stat as (
	select
		sch.flight_number,
		sch.origin_airport_code,
		sch.aircraft_type,
		fls.late_sum_min
	from alar.alar_staging_schedule sch
	left join flight_late_summury fls on sch.flight_number = fls.flight_number
)
select
	origin_airport_code,
	aircraft_type,
	sum(late_sum_min) as idle_time
from airport_late_stat
group by origin_airport_code, aircraft_type
order by origin_airport_code, sum(late_sum_min) desc nulls last
;