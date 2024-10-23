DROP TABLE IF EXISTS sssp_edge;
CREATE TABLE sssp_edge (
  src INT,
  dst INT,
  cost INT
);

CREATE TABLE sssp_base (
  dst INT,
  cost INT
);
CREATE TABLE sssp_recur (
  dst INT,
  cost INT
);
