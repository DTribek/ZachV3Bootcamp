insert into average_web_events
select 
	host,
	avg(num_hits)
from processed_events_aggregated_source
group by host