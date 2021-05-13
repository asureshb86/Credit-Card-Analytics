--hive commands to insert into core partition table

USE ccdms_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS `ccdms_data`(
  `trans_id` int, 
  `trans_date_trans_time` string, 
  `card_num` bigint, 
  `merchant` string, 
  `purchase_category` string, 
  `trans_amt` double, 
  `first_name` string, 
  `last_name` string, 
  `gender` string, 
  `street` string, 
  `city` string, 
  `state` string, 
  `zip` string, 
  `latitude` double, 
  `longitude` double, 
  `city_pop` int, 
  `job` string, 
  `dob` string, 
  `trans_num` string, 
  `unix_time` bigint, 
  `merch_lat` double, 
  `merch_long` double, 
  `debit_credit_indicator` string, 
  `trans_type` string, 
  `card_type` string)
PARTITIONED BY(
  `trans_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/ccdms_analysis.db/ccmds_data';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=3000;
set hive.exec.max.dyanamic.partitions.pernode=3000;

insert overwrite table ccdms_analysis.ccdms_data partition(trans_date) 
select trans_id, trans_date_trans_time, card_num, merchant, purchase_category, trans_amt, first_name, last_name, gender, street, city, state, zip, latitude, longitude, city_pop, job, dob, trans_num, unix_time, merch_lat, merch_long, debit_credit_indicator, trans_type, card_type, trans_date from temp_ccdms;


