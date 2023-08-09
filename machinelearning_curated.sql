CREATE EXTERNAL TABLE IF NOT EXISTS `jchaptostedi`.`machinelearning_curated` (
  `serialNumber` string,
  `sensorReadingTime` bigint,
  `distanceFromObject` int,
  `z` double,
  `timeStamp` bigint,
  `user` string,
  `y` double,
  `x` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-jchapto/machinelearning/curated/'
TBLPROPERTIES ('classification' = 'json');
