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

// "day", "month", "year", "Nombre d'articles", "Ton moyen", "country1", "country2"

CREATE TABLE queryd(
    day int,
    month int,
    year int,
    n_articles int,
    mean_tone float,
    country1 text [STATIC],
    country2 text [STATIC],
    PRIMARY KEY((country1, country2), mean_tone, n_articles))
WITH CLUSTERING ORDER BY (mean_tone DESC, n_articles, DESC);
