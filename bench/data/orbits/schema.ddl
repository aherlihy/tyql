DROP TABLE IF EXISTS orbits_base;
DROP TABLE IF EXISTS orbits_derived;
DROP TABLE IF EXISTS orbits_delta;
DROP TABLE IF EXISTS orbits_tmp;

CREATE TABLE orbits_base
(
    x TEXT,
    y TEXT
);

CREATE TABLE orbits_delta
(
    x TEXT,
    y TEXT
);
CREATE TABLE orbits_derived
(
    x TEXT,
    y TEXT
);
CREATE TABLE orbits_tmp
(
    x TEXT,
    y TEXT
);
