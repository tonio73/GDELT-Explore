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

"day", "month", "year", "Nombre d'articles", "Ton moyen", "country1", "country2"

CREATE TABLE queryd(
    day int,
    month int,
    year int,
    n_articles int,
    mean_tone float,
    country1 text,
    country2 text,
    PRIMARY KEY((country1, country2), mean_tone, n_articles))
WITH CLUSTERING ORDER BY (mean_tone DESC, n_articles, DESC);