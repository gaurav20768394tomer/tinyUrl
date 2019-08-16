CREATE KEYSPACE IF NOT EXISTS tinyurl
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

USE tinyurl;

CREATE TABLE IF NOT EXISTS urldata (
id                          text,
url                         text,
PRIMARY KEY (id)
) WITH
  bloom_filter_fp_chance=0.001000 AND
  caching={'keys': 'ALL' } AND
comment='' AND
gc_grace_seconds=10800 AND
default_time_to_live=0 AND
speculative_retry='99.0PERCENTILE' AND
memtable_flush_period_in_ms=0 AND
compaction={'class': 'LeveledCompactionStrategy'} AND
compression={'sstable_compression': 'LZ4Compressor'};


INSERT INTO urldata (id, url) VALUES('http://tiny.cc/ln55az', 'https://www.neueda.com/neueda-news/2019/6/3/neueda-open-sources-capital-markets-software');
INSERT INTO urldata (id, url) VALUES('https://www.neueda.com/neueda-news/2019/6/3/neueda-open-sources-capital-markets-software', 'http://tiny.cc/ln55az');