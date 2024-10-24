DROP TABLE IF EXISTS javapointsto_assign;
DROP TABLE IF EXISTS javapointsto_baseHPT;
DROP TABLE IF EXISTS javapointsto_loadT;
DROP TABLE IF EXISTS javapointsto_new;
DROP TABLE IF EXISTS javapointsto_store;
DROP TABLE IF EXISTS javapointsto_derived1;
DROP TABLE IF EXISTS javapointsto_derived2;
DROP TABLE IF EXISTS javapointsto_delta1;
DROP TABLE IF EXISTS javapointsto_delta2;
DROP TABLE IF EXISTS javapointsto_tmp1;
DROP TABLE IF EXISTS javapointsto_tmp2;

CREATE TABLE javapointsto_new
(
    x TEXT,
    y TEXT
);

CREATE TABLE javapointsto_assign
(
    x TEXT,
    y TEXT
);

CREATE TABLE javapointsto_loadT
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE javapointsto_store
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE javapointsto_hpt
(
    x TEXT,
    y TEXT,
    h TEXT
);

CREATE TABLE javapointsto_delta1
(
    x TEXT,
    y TEXT
);
CREATE TABLE javapointsto_derived1
(
    x TEXT,
    y TEXT
);
CREATE TABLE javapointsto_tmp1
(
    x TEXT,
    y TEXT
);
CREATE TABLE javapointsto_delta2
(
    x TEXT,
    y TEXT,
    h TEXT
);
CREATE TABLE javapointsto_derived2
(
    x TEXT,
    y TEXT,
    h TEXT
);
CREATE TABLE javapointsto_tmp2
(
    x TEXT,
    y TEXT,
    h TEXT
);
