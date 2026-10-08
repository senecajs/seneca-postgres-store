CREATE TABLE foo
(
  id character varying PRIMARY KEY,
  p1 character varying,
  p2 character varying,
  p3 character varying,
  x int,
  y int,
  int_arr integer[],
  "fooBar" character varying,
  bar_foo character varying,
  unique(x)
);

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

CREATE TABLE auto_incrementors
(
  id SERIAL PRIMARY KEY,
  value integer not null,
  unique(value)
);

CREATE TABLE products
(
  id character varying PRIMARY KEY,
  price decimal not null,
  label character varying default null,
  coolness_factor integer default null,
  unique(label),
  unique(label, price)
);

CREATE TABLE players
(
  id character varying PRIMARY KEY,
  username character varying not null,
  favorite_car character varying default null,
  points integer default null,
  points_history integer[] default null,
  unique(username)
);

CREATE TABLE racers
(
  id character varying PRIMARY KEY,
  points integer not null default 0,
  username character varying not null,
  favorite_car character varying not null,
  unique(username)
);

CREATE TABLE users
(
  id character varying PRIMARY KEY,
  username character varying not null,
  email character varying not null,
  unique(email)
);

CREATE TABLE customers
(
  id character varying PRIMARY KEY,
  first_name character varying not null,
  last_name character varying not null,
  credits integer not null,
  unique(first_name, last_name)
);

