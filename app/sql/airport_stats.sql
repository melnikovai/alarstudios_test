with flight_late_summury as (
	select
		flight_number,
		dow,
		sum(event_time - on_block_time) as late_sum_min
	from (
		select
			flight_number,
			movement_type,
			event_time,
			on_block_time,
			extract(isodow from event_date) as dow
		from alar.fct_flights_with_schedule_v
		where movement_type = 'arrival' and event_time - on_block_time >= interval '15' minute
	) as a
	group by flight_number, dow
), airport_late_stat as (
	select
		sch.flight_number,
		sch.origin_airport_code,
		sch.destination_airport_code,
		fls.dow,
		fls.late_sum_min
	from alar.alar_staging_schedule sch
	left join flight_late_summury fls on sch.flight_number = fls.flight_number
)
select
	destination_airport_code,
	dow,
	sum(late_sum_min) as late_sum_time
from airport_late_stat
group by destination_airport_code, dow
having sum(late_sum_min) is not null
order by destination_airport_code, late_sum_time desc;