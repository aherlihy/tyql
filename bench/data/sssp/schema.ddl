DROP TABLE IF EXISTS sssp_edge;
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