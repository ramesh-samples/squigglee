CREATE KEYSPACE ks_md WITH replication = {
  'class': 'NetworkTopologyStrategy', 'us-east': 8, 'us-west': 8, 'us-west-2': 8};

USE ks_md;

CREATE TABLE md (
  id bigint,
  datatype text,
  freq text,
  guid text,
  indexes text,
  ks text,
  ln int,
  replication text,
  startts bigint,
  strategy text,
  PRIMARY KEY ((id))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE INDEX md_datatype_idx ON md (datatype);

CREATE INDEX md_guid_idx ON md (guid);

CREATE INDEX md_ks_idx ON md (ks);

CREATE INDEX md_ln_idx ON md (ln);

CREATE INDEX md_startts_idx ON md (startts);

CREATE INDEX md_freq_idx ON md (freq);