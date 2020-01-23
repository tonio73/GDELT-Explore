CREATE KEYSPACE gdelt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };
USE gdelt;

CREATE TABLE querya(sqldate int,
    country text,
    lang text,
    PRIMARY KEY(sqldate, country));

CREATE TABLE group_by_month(
    country text,
    count int,
    sqldate text,
    year int,
    month int,
    day int,
    globaleventid int,
    PRIMARY KEY((globaleventid), count))
WITH CLUSTERING ORDER BY (count DESC);


// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);