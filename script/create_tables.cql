CREATE KEYSPACE gdelt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };
USE gdelt;

CREATE TABLE querya(sqldate int,
    country text,
    language text,
    count int,
    PRIMARY KEY((sqldate, country), count))
WITH CLUSTERING ORDER BY (count DESC);


CREATE TABLE queryb2(
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
    year text,
    month text,
    day text,
    source text,
    theme text,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((theme), nbarticle))
WITH CLUSTERING ORDER BY (nbarticle DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE querycperson(
    date int,
    year text,
    month text,
    day text,
    source text,
    person text,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((person), nbarticle))
WITH CLUSTERING ORDER BY (nbarticle DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE queryccountry(
    date int,
    year text,
    month text,
    day text,
    source text,
    country text,
    nbarticle int,
    tonemean float,
    PRIMARY KEY((country), nbarticle))
WITH CLUSTERING ORDER BY (nbarticle DESC);
// PRIMARY KEY(country, year, month, day), count) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE queryd2(
    day int,
    month int,
    year int,
    n_articles int,
    mean_tone float,
    country1 text ,
    country2 text ,
    PRIMARY KEY((country1, country2), mean_tone, n_articles))
WITH CLUSTERING ORDER BY (mean_tone DESC, n_articles DESC);