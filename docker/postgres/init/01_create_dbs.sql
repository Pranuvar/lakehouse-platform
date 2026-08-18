-- Runs once, on first container start, against the default DB created by
-- the POSTGRES_DB env var (the OLTP database). Creates the second logical
-- database used as Airflow's metadata store, so one Postgres instance
-- serves both roles instead of paying for a second container.
CREATE DATABASE airflow;

-- pgcrypto gives us gen_random_uuid() for surrogate keys in the OLTP schema.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Namespacing the source tables under their own schema (rather than
-- "public") mirrors how a real OLTP system is laid out, and keeps the
-- door open for adding a second application schema later without clashes.
CREATE SCHEMA IF NOT EXISTS oltp;
