--
-- run with psql -U postgres -f dbschema.sql
--
DROP DATABASE IF EXISTS senecatest;
CREATE DATABASE senecatest WITH ENCODING='UTF8' CONNECTION LIMIT=-1;

-- DROP ROLE senecatest;
CREATE ROLE "senecatest" LOGIN PASSWORD 'senecatest';

--# connect to the database (works for psql)
\c senecatest

CREATE TABLE foo
(
  id character varying PRIMARY KEY,
  p1 character varying,
  p2 character varying,
  p3 character varying,
  int_arr integer[],
  "fooBar" character varying,
  bar_foo character varying
);

ALTER TABLE foo OWNER TO senecatest;

CREATE TABLE moon_bar
(
  str character varying,
  id character varying PRIMARY KEY,
  "int" integer,
  bol boolean,
  wen timestamp with time zone,
  mark character varying,
  "dec" real,
  arr integer[],
  obj json,
  seneca text
);
ALTER TABLE moon_bar OWNER TO senecatest;

CREATE TABLE auto_incrementors
(
  id SERIAL PRIMARY KEY,
  value integer not null,
  unique(value)
);
ALTER TABLE auto_incrementors OWNER TO senecatest;

CREATE TABLE products
(
  id character varying PRIMARY KEY,
  price decimal not null,
  label character varying default null,
  coolness_factor integer default null,
  unique(label),
  unique(label, price)
);
ALTER TABLE products OWNER TO senecatest;

CREATE TABLE players
(
  id character varying PRIMARY KEY,
  username character varying not null,
  favorite_car character varying default null,
  points integer default null,
  points_history integer[] default null,
  unique(username)
);
ALTER TABLE players OWNER TO senecatest;

CREATE TABLE racers
(
  id character varying PRIMARY KEY,
  points integer not null default 0,
  username character varying not null,
  favorite_car character varying not null,
  unique(username)
);
ALTER TABLE racers OWNER TO senecatest;

CREATE TABLE users
(
  id character varying PRIMARY KEY,
  username character varying not null,
  email character varying not null,
  unique(email)
);
ALTER TABLE users OWNER TO senecatest;

CREATE TABLE customers
(
  id character varying PRIMARY KEY,
  first_name character varying not null,
  last_name character varying not null,
  credits integer not null,
  unique(first_name, last_name)
);
ALTER TABLE customers OWNER TO senecatest;

