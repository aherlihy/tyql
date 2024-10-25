DROP TABLE IF EXISTS trustchain_friends;
DROP TABLE IF EXISTS trustchain_delta1;
DROP TABLE IF EXISTS trustchain_derived1;
DROP TABLE IF EXISTS trustchain_tmp1;
DROP TABLE IF EXISTS trustchain_delta2;
DROP TABLE IF EXISTS trustchain_derived2;
DROP TABLE IF EXISTS trustchain_tmp2;


CREATE TABLE trustchain_friends
(
    person1 TEXT,
    person2 TEXT
);

CREATE TABLE trustchain_delta1
(
    person1 TEXT,
    person2 TEXT
);
CREATE TABLE trustchain_derived1
(
    person1 TEXT,
    person2 TEXT
);
CREATE TABLE trustchain_tmp1
(
    person1 TEXT,
    person2 TEXT
);

CREATE TABLE trustchain_delta2
(
    person1 TEXT,
    person2 TEXT
);
CREATE TABLE trustchain_derived2
(
    person1 TEXT,
    person2 TEXT
);
CREATE TABLE trustchain_tmp2
(
    person1 TEXT,
    person2 TEXT
);