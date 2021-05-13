#!bin/sh
####################################################
#                                                  #
# purpose: Data Ingestion from mysql to hive table #
#           using sqoop                            # 
# Version: 1.0                                     #
#                                                  #
####################################################


hiveURL=""
query="drop table if exists ccdms_analysis.temp_ccdms"

function checkError() {
      if [ $? -ne 0 ]
       then
          echo "Error Message: $1"
          exit 1
       else 
         echo "Success Message: $2"
       fi
}

function logmsg() {
 echo "logger: $1"
}
  
function drop_temp_table () {

  logmsg "Droping temp table: temp_ccdms"  
  hive --service beeline -u --silent=true --showWarnings=false "$hiveURL" -d "org.apache.hive.jdbc.hiveDriver" -e "$query"
  checkError "Error while droping temp table: temp_ccdms " "Drop table has been dropped Successfully"

}

function ingest_data_from_sqoop() {

logmsg "Ingesting data from mysql to hive using sqoop"
sqoop import --connect jdbc:mysql://localhost/project --table ccdms -m 2 --username root -P --hive-import --create-hive-table --hive-database ccdms_analysis --hive-table temp_ccdms
checkError "Error while ingesting data from mysql" "Successfully ingested Data"

}

function load_data_into_partition_tbl() {

logmsg "loading data into parition table: ccdms_data from temp_table: temp_ccdms"
hive --service beeline -u --silent=true --showWarnings=false "$hiveURL" -d "org.apache.hive.jdbc.hiveDriver" -f "/build/CCMDSReport/hive-scripts/load_data_into_partition_tbl.hql"
checkError "Error while loading data into partition table " "Successfully loaded data into parition table: ccdms_data from temp_table: temp_ccdms"

}

drop_temp_table
ingest_data_from_sqoop
load_data_into_partition_tbl


