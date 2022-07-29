create external table dl_bronze.players_nba(
id int,
first_name string,
height_feet int,
height_inches int,
last_name string,
position string,
weight_pounds int,
team_id int,
team_abbreviation string,
team_city string,
team_conference string,
team_division string,
team_full_name string,
team_name string
)
PARTITIONED BY (date_inserted timestamp)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/data/api/nba/players/'
TBLPROPERTIES ('transactional'='false')