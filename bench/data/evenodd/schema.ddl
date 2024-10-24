DROP TABLE IF EXISTS evenodd_numbers;
DROP TABLE IF EXISTS evenodd_delta0;
DROP TABLE IF EXISTS evenodd_derived0;
DROP TABLE IF EXISTS evenodd_tmp0;
DROP TABLE IF EXISTS evenodd_delta1;
DROP TABLE IF EXISTS evenodd_derived1;
DROP TABLE IF EXISTS evenodd_tmp1;


CREATE TABLE evenodd_numbers
(
    id    INTEGER,
    value INTEGER
);
CREATE TABLE evenodd_delta2
(
    value INTEGER,
    typ   TEXT
);
CREATE TABLE evenodd_derived2
(
    value INTEGER,
    typ   TEXT
);
CREATE TABLE evenodd_tmp2
(
    value INTEGER,
    typ   TEXT
);
CREATE TABLE evenodd_delta1
(
    value INTEGER,
    typ   TEXT
);
CREATE TABLE evenodd_derived1
(
    value INTEGER,
    typ   TEXT
);
CREATE TABLE evenodd_tmp1
(
    value INTEGER,
    typ   TEXT
);