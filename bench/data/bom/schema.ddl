DROP TABLE IF EXISTS bom_assbl;
DROP TABLE IF EXISTS bom_basic;
DROP TABLE IF EXISTS bom_delta;
DROP TABLE IF EXISTS bom_derived;
DROP TABLE IF EXISTS bom_tmp;

CREATE TABLE bom_assbl
(
    part  TEXT,
    spart TEXT
);
CREATE TABLE bom_basic
(
    part TEXT,
    days INT
);
CREATE TABLE bom_delta
(
    part TEXT,
    max  INT
);
CREATE TABLE bom_derived
(
    part TEXT,
    max  INT
);
CREATE TABLE bom_tmp
(
    part TEXT,
    max  INT
);
