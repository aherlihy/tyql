DROP TABLE IF EXISTS sssp_edge;
DROP TABLE IF EXISTS sssp_base;
DROP TABLE IF EXISTS sssp_delta;
DROP TABLE IF EXISTS sssp_derived;
DROP TABLE IF EXISTS sssp_tmp;

CREATE TABLE sssp_edge
(
    src  INT,
    dst  INT,
    cost INT
);

CREATE TABLE sssp_base
(
    dst  INT,
    cost INT
);

CREATE TABLE sssp_delta
(
    dst  INT,
    cost INT
);
CREATE TABLE sssp_derived
(
    dst  INT,
    cost INT
);
CREATE TABLE sssp_tmp
(
    dst  INT,
    cost INT
);
