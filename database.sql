CREATE DATABASE "Timely"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1;

--
DROP TABLE jobs;
DROP TABLE job_schedule;

CREATE TABLE IF NOT EXISTS job_schedule
(
    id UUID NOT NULL PRIMARY KEY,
    description CHARACTER VARYING(1024),
    status CHARACTER VARYING(64) NOT NULL,
    frequency CHARACTER VARYING(256) NOT NULL,
    attempt INT,
    retry_policy_strategy CHARACTER VARYING(32),
    retry_policy_count INT,
    retry_policy_interval CHARACTER VARYING(32),
    last_execution_date TIMESTAMP WITH TIME ZONE,
    next_execution_date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS jobs
(
    id UUID NOT NULL PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES job_schedule(id),
    slug CHARACTER VARYING(256) NOT NULL,
    data TEXT
);

CREATE TABLE IF NOT EXISTS job_history
(
    id UUID NOT NULL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    schedule_id UUID NOT NULL REFERENCES job_schedule(id),
    status CHARACTER VARYING(128) NOT NULL,
    reason CHARACTER VARYING(1024),
    attempt INT NOT NUll,
    execution_date TIMESTAMP WITH TIME ZONE
);