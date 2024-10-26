DROP TABLE IF EXISTS cba_term;
DROP TABLE IF EXISTS cba_lits;
DROP TABLE IF EXISTS cba_vars;
DROP TABLE IF EXISTS cba_abs;
DROP TABLE IF EXISTS cba_app;
DROP TABLE IF EXISTS cba_baseCtrl;
DROP TABLE IF EXISTS cba_baseData;

DROP TABLE IF EXISTS cba_delta1;
DROP TABLE IF EXISTS cba_delta2;
DROP TABLE IF EXISTS cba_delta3;
DROP TABLE IF EXISTS cba_delta4;

DROP TABLE IF EXISTS cba_derived1;
DROP TABLE IF EXISTS cba_derived2;
DROP TABLE IF EXISTS cba_derived3;
DROP TABLE IF EXISTS cba_derived4;

DROP TABLE IF EXISTS cba_tmp1;
DROP TABLE IF EXISTS cba_tmp2;
DROP TABLE IF EXISTS cba_tmp3;
DROP TABLE IF EXISTS cba_tmp4;

CREATE TABLE cba_term
(
    x INTEGER,
    y TEXT,
    z INTEGER
);

CREATE TABLE cba_lits
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_vars
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_abs
(
    x INTEGER,
    y INTEGER,
    z INTEGER
);
CREATE TABLE cba_app
(
    x INTEGER,
    y INTEGER,
    z INTEGER
);
CREATE TABLE cba_baseCtrl
(
    x INTEGER,
    y INTEGER
);
CREATE TABLE cba_baseData
(
    x INTEGER,
    y TEXT
);

CREATE TABLE cba_delta1
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_delta2
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_delta3
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_delta4
(
    x INTEGER,
    y TEXT
);

CREATE TABLE cba_derived1
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_derived2
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_derived3
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_derived4
(
    x INTEGER,
    y TEXT
);

CREATE TABLE cba_tmp1
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_tmp2
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_tmp3
(
    x INTEGER,
    y TEXT
);
CREATE TABLE cba_tmp4
(
    x INTEGER,
    y TEXT
);
