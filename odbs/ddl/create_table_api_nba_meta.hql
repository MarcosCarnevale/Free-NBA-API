create external table dl_bronze.metadata_nba(
total_pages int,
current_page int,
next_page int,
per_page int,
total_count int
)
PARTITIONED BY (date_inserted timestamp)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/data/api/nba/meta/'
TBLPROPERTIES ('transactional'='false')