DROP TABLE IF EXISTS parents;

CREATE TABLE ancestry_parents (
  parent TEXT,
  child TEXT
);

CREATE TABLE ancestry_base (
  name TEXT,
  gen INT
);

CREATE TABLE ancestry_recur (
  name TEXT,
  gen INT
);
