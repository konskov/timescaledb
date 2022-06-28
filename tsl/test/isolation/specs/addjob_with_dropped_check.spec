# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    CREATE OR REPLACE PROCEDURE test_proc_with_check(job_id int, config jsonb)
    LANGUAGE PLPGSQL
    AS $$
    BEGIN
        perform least(1, 2);
    END
    $$;
}

teardown {
    DROP PROCEDURE test_proc_with_check;
    DROP FUNCTION IF EXISTS test_config_check_func;
}

session "S"
step "Sinit_func" 
{
    CREATE OR REPLACE FUNCTION test_config_check_func(config jsonb) RETURNS VOID
    AS $$
    BEGIN 
        perform least(1, 2);
    END
    $$ LANGUAGE PLPGSQL;
}
step "Sinit_addjob" { select add_job('test_proc_with_check', '5 secs', config => NULL, check_config => 'test_config_check_func'::regproc); }
step "Sb"	{ BEGIN; }
step "S1"	{ select add_job('test_proc_with_check', '5 secs', config => NULL, check_config => 'test_config_check_func'::regproc); }
step "Sc"	{ COMMIT; }

session "D"
step "D1"	{ DROP FUNCTION IF EXISTS test_config_check_func; }

permutation "Sinit_func" "Sinit_addjob" "Sb" "D1" "S1" "Sc"

