create schema if not exists datalake;

create table datalake.sensor_values (
ts timestamp with time zone not null
,machine_id text not null
,message_key  character varying(255)               not null  
,value_int    integer                                        
,value_float  double precision                               
,value_text text
);

SELECT create_hypertable('datalake.sensor_values', 'ts');

ALTER TABLE datalake.sensor_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts DESC',
  timescaledb.compress_segmentby = 'machine_id, message_key'
);

--- maybe if I try to reverse the order of the segbys?
ALTER TABLE datalake.sensor_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts DESC',
  timescaledb.compress_segmentby = 'message_key, machine_id'
);

SELECT add_compression_policy('datalake.sensor_values', INTERVAL '7 days');

COPY datalake.sensor_values from '/home/ntina/Downloads/bugreport/svc.sql';

create table datalake.sensor_values_latest (
	ts           timestamp with time zone             not null  
	,machine_id   text                                 not null  
	,message_key  character varying(255)               not null  
	,value_int    integer                                        
	,value_float  double precision                               
	,value_text   text                                           
	,received_ts  timestamptz not null default now()
);

COPY datalake.sensor_values from '/home/ntina/Downloads/bugreport/svc.sql';
COPY datalake.sensor_values_latest from '/home/ntina/Downloads/bugreport/latest.sql';

create index on datalake.sensor_values_latest (machine_id);
create unique index on datalake.sensor_values_latest (machine_id, message_key);
create index on datalake.sensor_values_latest (machine_id, message_key, ts DESC);
create index on datalake.sensor_values_latest (message_key);

SELECT compress_chunk('_timescaledb_internal.' || cast(chunk_name as text))
FROM chunk_compression_stats('datalake.sensor_values')
WHERE compression_status = 'Uncompressed';

with query_params as (
	select distinct machine_id, message_key 
	from datalake.sensor_values_latest
	where sensor_values_latest.message_key IN ('epec_supply', 'latitude', 'longitude') 
	   and sensor_values_latest.machine_id IN ('123')
)
select
   sv.ts,
   sv.message_key = q.message_key as "THIS SHOULD NEVER BE FALSE",
   sv.machine_id,
   sv.message_key,
   sv.value_int,
   sv.value_float,
   sv.value_text
   , q.*

from datalake.sensor_values sv
inner join query_params q 
	on q.machine_id = sv.machine_id
	and q.message_key = sv.message_key
where sv.ts between '2023-02-01' and '2023-02-02'
order by sv.ts desc
limit 1000;

---------------- and a query that works
explain
select distinct machine_id, message_key from
datalake.sensor_values
where sensor_values.message_key in ('epec_supply', 'latitude', 'longitude')
and sensor_values.machine_id IN ('123') and sensor_values.ts between '2023-02-01' and '2023-02-02';


---------- gayathri's example from https://github.com/timescale/timescaledb/commit/6dad1f246a961444b21f9fdbcac51bc5b9196491#diff-5e7bc03c2fd2235f31be0a0349f7c0c72f8af9fde9873d5d05e1dff433e8e3ea
create table nodetime( node int,
     start_time timestamp ,
     stop_time timestamp  );
insert into nodetime values( 4, '2018-01-06 00:00'::timestamp, '2018-12-02 12:00'::timestamp);

set enable_seqscan = false;
set enable_bitmapscan = false;
set max_parallel_workers_per_gather = 0;
set enable_hashjoin = false;
set enable_mergejoin = false;

select device_id, count(*) from
(select * from metrics_ordered_idx mt, nodetime nd 
where mt.time > nd.start_time and mt.device_id = nd.node and mt.time < nd.stop_time) as subq group by device_id;