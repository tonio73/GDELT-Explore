CREATE KEYSPACE gdelt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };
USE gdelt;

CREATE TABLE querya(sqldate int,
    country text,
    language text,
    count int,
    PRIMARY KEY((sqldate, country), count))
WITH CLUSTERING ORDER BY (count DESC);


CREATE TABLE queryb(
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