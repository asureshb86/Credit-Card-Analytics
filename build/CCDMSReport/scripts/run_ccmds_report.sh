#!bin/sh
######################################################
#                                                    #
# purpose: 1.performing transformations using spark  #
#          code and loading into temp table          #
#          2.Loading data into partition table       #
#            from temp table                         #
#          3.Generate final report for visualization #
#                                                    #
# Version: 1.0                                       #
#                                                    #
######################################################



hiveURL=""
report_gen_date=$1

if [ "$report_gen_date" == "" ]
 then
   echo "Please pass report Generation date"
   exit 1
fi


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


function run_spark_code_for_transforamtions() {

logmsg "Running Spark code by using spark-submit command"
sh /usr/bin/spark-submit --class com.wfs.ccdms.analytics.CCDMSReportGenerator --name GeneratorCCDMSReportByCustomer --master yarn /home/cloudera/project_work/ccdms-spark-services.jar  "2020-12-01" "2020-12-031" "Joanne" "Williams" "ccdms_analysis.ccdms_data"
checkError "Error while running Spark code" "Spark Code Executed Successfully"

}

function load_data_into_partition_table() {

logmsg "loading Customer Report into parition table: ccdms_customer_report from temp_table: temp_cusomter_report"
hive --service beeline -u --silent=true --showWarnings=false "$hiveURL" -d "org.apache.hive.jdbc.hiveDriver" -f "/build/CCMDSReport/hive-scripts/load_customer_report.hql"
checkError "Error while loading Customer report into partition table: ccdms_customer_report " "Successfully loaded Customer report into parition table: ccdms_customer_report from temp_table: temp_cusomter_report"

}

function generate_final_report() {

column_names="customer_name,transaction_category,total_amount_on_category,reward_points_accured,date_range,report_gen_date"

logmsg " Generating final customer Report "
hive --service beeline -u --silent=true --showWarnings=false "$hiveURL" -d "org.apache.hive.jdbc.hiveDriver" -e "select $column_names from ccdms_analysis.ccdms_report_data where report_gen_date=$report_gen_date" > "/build/CCMDSReport/customer-reports/generate_customer_report.csv"
checkError "Error while loading final customer Report " "Successfully generated customer report "

}

run_spark_code_for_transforamtions
load_data_into_partition_table
generate_final_report


