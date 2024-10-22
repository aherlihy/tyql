DROP TABLE IF EXISTS tc_edge;

CREATE TABLE tc_edge (
  x INTEGER, y INTEGER
);
CREATE TABLE tc_path (
  startNode INTEGER, endNode INTEGER, path INTEGER[]
);
CREATE TABLE tc_path_temp (
  startNode INTEGER, endNode INTEGER, path INTEGER[]
);
