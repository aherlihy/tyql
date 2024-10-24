DROP TABLE IF EXISTS pointstocount_assign;
DROP TABLE IF EXISTS pointstocount_baseHPT;
DROP TABLE IF EXISTS pointstocount_loadT;
DROP TABLE IF EXISTS pointstocount_new;
DROP TABLE IF EXISTS pointstocount_store;
DROP TABLE IF EXISTS pointstocount_derived1;
DROP TABLE IF EXISTS pointstocount_derived2;
DROP TABLE IF EXISTS pointstocount_delta1;
DROP TABLE IF EXISTS pointstocount_delta2;
DROP TABLE IF EXISTS pointstocount_tmp1;
DROP TABLE IF EXISTS pointstocount_tmp2;

CREATE TABLE pointstocount_new
(
    x TEXT,
    y TEXT
);

CREATE TABLE pointstocount_assign
(
    x TEXT,
    y TEXT
);

CREATE TABLE pointstocount_loadT
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE pointstocount_store
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE pointstocount_hpt
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE pointstocount_delta1
(
    x TEXT,
    y TEXT
);
CREATE TABLE pointstocount_derived1
(
    x TEXT,
    y TEXT
);
CREATE TABLE pointstocount_tmp1
(
    x TEXT,
    y TEXT
);
CREATE TABLE pointstocount_delta2
(
    x TEXT,
    y TEXT,
    h TEXT
);
CREATE TABLE pointstocount_derived2
(
    x TEXT,
    y TEXT,
    h TEXT
);
CREATE TABLE pointstocount_tmp2
(
    x TEXT,
    y TEXT,
    h TEXT
);
