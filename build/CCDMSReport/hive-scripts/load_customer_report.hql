--load customer report into partition table
create EXTERNAL TABLE IF NOT EXISTS ccdms_analysis.ccdms_customer_report(
`customer_name` string,
`transaction_category` string,
`total_amount_on_category` double,
`reward_points_accured` int,
`date_range` string)
PARTITIONED BY
(`report_gen_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/ccdms_analysis.db/ccdms_customer_report';



INSERT OVERWRITE TABLE ccdms_analysis.ccdms_customer_report(report_gen_date)
SELECT customer_name,
       transcation_category,
       sum(amount_spent) OVER(PARITION BY transcation_category,customer_name,date_range) AS total_amount_on_category,
       sum(reward_points_accured) OVER(PARITION BY transcation_category,customer_name,date_range) AS reward_points_accured,
       date_range,
       report_gen_date
FROM ccdms_analysis.temp_cusomter_report;


