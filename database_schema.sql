CREATE DATABASE "Timely"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1;

--
DROP TABLE IF EXISTS job_runs;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS schedules;

CREATE TABLE IF NOT EXISTS schedules
(
    id UUID NOT NULL PRIMARY KEY,
    group_id UUID NOT NULL,
    description CHARACTER VARYING(1024),
    status CHARACTER VARYING(64) NOT NULL,
    frequency CHARACTER VARYING(256) NOT NULL,
    schedule_start TIMESTAMP WITH TIME ZONE,
    retry_policy_strategy CHARACTER VARYING(32),
    retry_policy_count INT,
    retry_policy_interval CHARACTER VARYING(32),
    transport_type CHARACTER VARYING(32),
    url CHARACTER VARYING(1024),
    last_execution_date TIMESTAMP WITH TIME ZONE,
    next_execution_date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS jobs
(
    id UUID NOT NULL PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    slug CHARACTER VARYING(256) NOT NULL,
    data TEXT
);

CREATE TABLE IF NOT EXISTS job_runs
(
    id UUID NOT NULL PRIMARY KEY,
    group_id UUID NOT NULL,
    schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    status CHARACTER VARYING(128) NOT NULL,
    reason CHARACTER VARYING(1024),
    start_date TIMESTAMP WITH TIME ZONE NOT NULL,
    end_date TIMESTAMP WITH TIME ZONE
);