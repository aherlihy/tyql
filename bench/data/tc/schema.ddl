DROP TABLE IF EXISTS tc_edge;

CREATE TABLE tc_edge
(
    x INTEGER,
    y INTEGER
);
CREATE TABLE tc_derived
(
    startNode INTEGER,
    endNode   INTEGER,
    path      INTEGER[]
);
CREATE TABLE tc_delta
(
    startNode INTEGER,
    endNode   INTEGER,
    path      INTEGER[]
);
CREATE TABLE tc_tmp
(
    startNode INTEGER,
    endNode   INTEGER,
    path      INTEGER[]
);
