DROP TABLE IF EXISTS cc_shares;
DROP TABLE IF EXISTS cc_control;
DROP TABLE IF EXISTS cc_derived1;
DROP TABLE IF EXISTS cc_derived2;
DROP TABLE IF EXISTS cc_delta1;
DROP TABLE IF EXISTS cc_delta2;
DROP TABLE IF EXISTS cc_tmp1;
DROP TABLE IF EXISTS cc_tmp2;

CREATE TABLE cc_shares
(
    byC     TEXT,
    of      TEXT,
    percent INT
);

CREATE TABLE cc_empty_shares
(
    byC     TEXT,
    of      TEXT,
    percent INT
);

CREATE TABLE cc_control
(
    com1 TEXT,
    com2 TEXT
);

CREATE TABLE cc_delta1
(
    byC     TEXT,
    of      TEXT,
    percent INT
);

CREATE TABLE cc_delta2
(
    com1 TEXT,
    com2 TEXT
);
CREATE TABLE cc_derived1
(
    byC     TEXT,
    of      TEXT,
    percent INT
);

CREATE TABLE cc_derived2
(
    com1 TEXT,
    com2 TEXT
);
CREATE TABLE cc_tmp1
(
    byC     TEXT,
    of      TEXT,
    percent INT
);

CREATE TABLE cc_tmp2
(
    com1 TEXT,
    com2 TEXT
);