insert into average_web_events
select 
	host,
	avg(num_hits),
	date(event_time_stamp) as event_day
from processed_events_aggregated_source
group by host, date(event_time_stamp)