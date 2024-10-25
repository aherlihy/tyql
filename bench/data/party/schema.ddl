DROP TABLE IF EXISTS party_organizer;
DROP TABLE IF EXISTS party_friends;
DROP TABLE IF EXISTS party_counts;
DROP TABLE IF EXISTS party_delta1;
DROP TABLE IF EXISTS party_derived1;
DROP TABLE IF EXISTS party_tmp1;
DROP TABLE IF EXISTS party_delta2;
DROP TABLE IF EXISTS party_derived2;
DROP TABLE IF EXISTS party_tmp2;



CREATE TABLE party_organizers
(
    OrgName TEXT
);

CREATE TABLE party_friends
(
    Pname TEXT,
    Fname TEXT
);

CREATE TABLE party_counts
(
    fName  TEXT,
    nCount INT
);

CREATE TABLE party_delta2
(
    fName TEXT,
    nCount  INT
);
CREATE TABLE party_derived2
(
    fName TEXT,
    nCount  INT
);
CREATE TABLE party_tmp2
(
    fName TEXT,
    nCount  INT
);

CREATE TABLE party_delta1
(
    Person TEXT
);
CREATE TABLE party_derived1
(
    Person TEXT
);
CREATE TABLE party_tmp1
(
    Person TEXT
);


