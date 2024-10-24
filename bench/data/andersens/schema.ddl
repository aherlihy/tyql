DROP TABLE IF EXISTS andersens_addressOf;
DROP TABLE IF EXISTS andersens_assign;
DROP TABLE IF EXISTS andersens_loadT;
DROP TABLE IF EXISTS andersens_store;
DROP TABLE IF EXISTS andersens_delta;
DROP TABLE IF EXISTS andersens_derived;
DROP TABLE IF EXISTS andersens_tmp;

CREATE TABLE andersens_addressOf
(
    x TEXT,
    y TEXT
);

CREATE TABLE andersens_assign
(
    x TEXT,
    y TEXT
);

CREATE TABLE andersens_loadT
(
    x TEXT,
    y TEXT
);

CREATE TABLE andersens_store
(
    x TEXT,
    y TEXT
);
CREATE TABLE andersens_derived
(
    x TEXT,
    y TEXT
);
CREATE TABLE andersens_delta
(
    x TEXT,
    y TEXT
);
CREATE TABLE andersens_tmp
(
    x TEXT,
    y TEXT
);