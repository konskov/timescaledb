CREATE TYPE custom_type AS (high int, low int);

CREATE TABLE conditions (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null,
      highlow     custom_type null,
      bit_int     smallint,
      good_life   boolean
    );

SELECT table_name FROM create_hypertable( 'conditions', 'timec');

INSERT INTO conditions
SELECT generate_series('2018-10-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 second'), 'POR', 55, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO conditions
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 second'), 'NYC', 35, 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO conditions
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 second'), 'LA', 73, 55, NULL, 28, NULL, NULL, 8, true;

CREATE MATERIALIZED VIEW cagg1 
WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', timec) as bucket,
	location,
	-- round(min(allnull)) as min_allnull,
	round(max(temperature)) as max_temp,
	round(sum(temperature)+sum(humidity)) as agg_sum_expr,
	round(avg(humidity)) AS avg_humidity,
	round(stddev(humidity)) as stddev_humidity,
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
FROM conditions
GROUP BY bucket, location HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;

CREATE MATERIALIZED VIEW cagg2 
WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', timec) as bucket,
	location,
	round(min(allnull)) as min_allnull,
	-- round(max(temperature)) as max_temp,
	round(sum(temperature)+sum(humidity)) as agg_sum_expr,
	round(avg(humidity)) AS avg_humidity,
	round(stddev(humidity)) as stddev_humidity,
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
FROM conditions
GROUP BY bucket, location HAVING min(location) >= 'LA' and avg(temperature) > 2 WITH NO DATA;

CREATE MATERIALIZED VIEW cagg3 
WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', timec) as bucket,
	location,
	round(min(allnull)) as min_allnull,
	round(max(temperature)) as max_temp,
	round(sum(temperature)+sum(humidity)) as agg_sum_expr,
	-- round(avg(humidity)) AS avg_humidity,
	round(stddev(humidity)) as stddev_humidity,
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
FROM conditions
GROUP BY bucket, location HAVING min(location) >= 'POR' and avg(temperature) > 2 WITH NO DATA;


---------- now refresh them simultaneously, see if there's contention and difference in time
call refresh_continuous_aggregate('cagg1', NULL, NULL);
call refresh_continuous_aggregate('cagg2', NULL, NULL);
call refresh_continuous_aggregate('cagg3', NULL, NULL);

-- schedule them to run every 90 seconds, and a refresh will last about 40 seconds. 
select add_continuous_aggregate_policy('cagg1', NULL, NULL, INTERVAL '10 minutes');
select add_continuous_aggregate_policy('cagg2', NULL, NULL, INTERVAL '10 minutes');
select add_continuous_aggregate_policy('cagg3', NULL, NULL, INTERVAL '10 minutes');


INSERT INTO conditions
SELECT generate_series('2020-10-01 00:00'::timestamp, '2020-12-31 00:00'::timestamp, '5 seconds'), 'POR', 55, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO conditions
SELECT generate_series('2020-11-01 00:00'::timestamp, '2020-12-31 00:00'::timestamp, '5 seconds'), 'NYC', 35, 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO conditions
SELECT generate_series('2018-11-01 00:00'::timestamp, '2020-12-15 00:00'::timestamp, '5 seconds'), 'LA', 73, 55, NULL, 28, NULL, NULL, 8, true;

---
CREATE MATERIALIZED VIEW cagg1_1
WITH (timescaledb.continuous) AS
SELECT time_bucket('1day', timec) as bucket,
	location,
	-- round(min(allnull)) as min_allnull,
	round(max(temperature)) as max_temp,
	round(sum(temperature)+sum(humidity)) as agg_sum_expr,
	round(avg(humidity)) AS avg_humidity,
	round(stddev(humidity)) as stddev_humidity,
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
FROM conditions
GROUP BY bucket, location HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;

select add_continuous_aggregate_policy('cagg1_1', NULL, NULL, INTERVAL '90 seconds');


-------------- try with caggs that have updates in similar regions ----------------------
-------------- try with caggs that have updates in different regions ------------------
-------------- try with two concurrent caggs and then try with more than 2 concurrent caggs -----------
-------------- baseline is always the performance with caggs refreshing serially, and manually called ---
select * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log ;

select * from _timescaledb_catalog.continuous_aggs_invalidation_threshold ;

select * from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

---- serially -----
DO $$
DECLARE
	arr int[] := array[1,2,3];
	caggno int;
	cagg_name text;
BEGIN
	FOREACH caggno in array arr
	LOOP
		raise notice 'caggno is %', caggno;
		cagg_name := 'cagg' || caggno; 
		raise notice 'cagg_name is %', cagg_name;
		call refresh_continuous_aggregate(cagg_name, NULL, NULL);
	END LOOP;
END $$;

DO $$
DECLARE
  cagg _timescaledb_catalog.continuous_agg%ROWTYPE;
  dimrow _timescaledb_catalog.dimension%ROWTYPE;
  end_val bigint;
  getendval text;
BEGIN
    FOR cagg in SELECT * FROM _timescaledb_catalog.continuous_agg
    LOOP
        SELECT * INTO dimrow 
        FROM _timescaledb_catalog.dimension dim 
        WHERE dim.hypertable_id = cagg.raw_hypertable_id AND dim.num_slices IS NULL AND dim.interval_length IS NOT NULL;

        IF dimrow.column_type IN  ('TIMESTAMP'::regtype, 'DATE'::regtype, 'TIMESTAMPTZ'::regtype)
        THEN
            IF cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807 
            THEN
                end_val := -210866803200000001;
            ELSE
                end_val := (extract(epoch from now()) * 1000000 - cagg.ignore_invalidation_older_than)::int8;
            END IF;
        ELSE
            IF cagg.ignore_invalidation_older_than IS NULL OR cagg.ignore_invalidation_older_than = 9223372036854775807 
            THEN
                end_val := -2147483649;
            ELSE
                getendval := format('SELECT %s.%s() - %s', dimrow.integer_now_func_schema, dimrow.integer_now_func, cagg.ignore_invalidation_older_than);
                EXECUTE getendval INTO end_val;
            END IF;
        END IF;

        INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log 
          VALUES (cagg.mat_hypertable_id, -9223372036854775808, -9223372036854775808, end_val);
    END LOOP;
END $$;



---------------- serially refresh the continuous aggregates
---- serially, this is the baseline -----
DO $$
DECLARE
	arr int[] := array[1,2,3];
	caggno int;
	cagg_name text;
BEGIN
	FOREACH caggno in array arr
	LOOP
		raise notice 'caggno is %', caggno;
		cagg_name := 'cagg' || caggno; 
		raise notice 'cagg_name is %', cagg_name;
		call refresh_continuous_aggregate(cagg_name, NULL, NULL);
	END LOOP;
END $$;

-- indeed refreshing even one of the caggs, will update the cagg_invalidation log for all the caggs that 
-- share the same underlying hypertable. So concurrent cagg refresh execution at the cagg invalidation log update stage 
-- is the source of extra overhead.  
--------- concurrently, caggs 1+2, then cagg3 by itself
