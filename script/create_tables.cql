CREATE KEYSPACE gdelt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };
USE gdelt;

CREATE TABLE querya(sqldate int,
    country text,
    lang text
    PRIMARY KEY(sqldate, country));

CREATE TABLE queryb(
    country text,
    count int,
    sqldate int,
    year int,
    month int,
    day,
    globaleventid int,
    PRIMARY KEY(country), count)
WITH CLUSTERING ORDER BY (count DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);