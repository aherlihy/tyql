DROP TABLE IF EXISTS parents;

CREATE TABLE ancestry_parents
(
    parent TEXT,
    child  TEXT
);

CREATE TABLE ancestry_delta
(
    name TEXT,
    gen  INT
);

CREATE TABLE ancestry_derived
(
    name TEXT,
    gen  INT
);
CREATE TABLE ancestry_tmp
(
    name TEXT,
    gen  INT
);
