DROP TABLE IF EXISTS asps_edge;
DROP TABLE IF EXISTS asps_delta;
DROP TABLE IF EXISTS asps_derived;
DROP TABLE IF EXISTS asps_tmp;
CREATE TABLE asps_edge
(
    src  INT,
    dst  INT,
    cost INT
);

CREATE TABLE asps_delta
(
    src  INT,
    dst INT,
    cost INT
);
CREATE TABLE asps_derived
(
    src  INT,
    dst INT,
    cost INT
);
CREATE TABLE asps_tmp
(
    src  INT,
    dst INT,
    cost INT
);
