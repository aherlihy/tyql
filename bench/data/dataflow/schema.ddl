DROP TABLE IF EXISTS dataflow_readOp;
DROP TABLE IF EXISTS dataflow_writeOp;
DROP TABLE IF EXISTS dataflow_jumpOp;
DROP TABLE IF EXISTS dataflow_delta;
DROP TABLE IF EXISTS dataflow_derived;
DROP TABLE IF EXISTS dataflow_tmp;

CREATE TABLE dataflow_readOp
(
    opN  TEXT,
    varN TEXT
);

CREATE TABLE dataflow_writeOp
(
    opN  TEXT,
    varN TEXT
);

CREATE TABLE dataflow_jumpOp
(
    a TEXT,
    b TEXT
);

CREATE TABLE dataflow_delta
(
    a TEXT,
    b TEXT
);

CREATE TABLE dataflow_derived
(
    a TEXT,
    b TEXT
);

CREATE TABLE dataflow_tmp
(
    a TEXT,
    b TEXT
);