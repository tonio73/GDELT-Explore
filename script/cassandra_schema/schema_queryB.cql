
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE test.queryb (
    globaleventid int PRIMARY KEY,
    actiongeo_adm1code text,
    actiongeo_adm2code text,
    actiongeo_countrycode text,
    actiongeo_featureid text,
    actiongeo_fullname text,
    actiongeo_lat double,
    actiongeo_long double,
    actiongeo_type int,
    actor1code text,
    actor1countrycode text,
    actor1ethniccode text,
    actor1geo_adm1code text,
    actor1geo_adm2code text,
    actor1geo_countrycode text,
    actor1geo_featureid text,
    actor1geo_fullname text,
    actor1geo_lat double,
    actor1geo_long double,
    actor1geo_type int,
    actor1knowngroupcode text,
    actor1name text,
    actor1religion1code text,
    actor1religion2code text,
    actor1type1code text,
    actor1type2code text,
    actor1type3code text,
    actor2code text,
    actor2countrycode text,
    actor2ethniccode text,
    actor2geo_adm1code text,
    actor2geo_adm2code text,
    actor2geo_countrycode text,
    actor2geo_featureid text,
    actor2geo_fullname text,
    actor2geo_lat double,
    actor2geo_long double,
    actor2geo_type int,
    actor2knowngroupcode text,
    actor2name text,
    actor2religion1code text,
    actor2religion2code text,
    actor2type1code text,
    actor2type2code text,
    actor2type3code text,
    avgtone double,
    dateadded bigint,
    eventbasecode text,
    eventcode text,
    eventrootcode text,
    fractiondate int,
    goldsteinscale double,
    isrootevent int,
    monthyear int,
    numarticles int,
    nummentions int,
    numsources int,
    quadclass int,
    sourceurl text,
    sqldate int,
    year int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE test.kv (
    key text PRIMARY KEY,
    value int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

