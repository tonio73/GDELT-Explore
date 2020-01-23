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
    sqldate int,
    year int,
    month int,
    day int,
    count int,
    globaleventid int,
    PRIMARY KEY((country), count))
WITH CLUSTERING ORDER BY (count DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE queryctheme(
    date int,
    source string,
    theme string,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((country), count))
WITH CLUSTERING ORDER BY (nbartcile DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE querycperson(
    date int,
    source string,
    person string,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((country), count))
WITH CLUSTERING ORDER BY (nbartcile DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE queryccountry(
    date int,
    source string,
    country string,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((country), count))
WITH CLUSTERING ORDER BY (nbartcile DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);