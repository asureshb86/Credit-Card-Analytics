package com.wfs.ccdms.analytics.utils

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object CommonUtils {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def extractData(spark: SparkSession, tableName: String, startDate: String, endDate: String, firstName: String, lastName: String): DataFrame = {

    var query = "select * from " + tableName + " where businessdate >= " + startDate + " and businessdate <= " + endDate + " and first_name = " + firstName + " and last_name = " +lastName;
    val queryResultsDF = spark.sql(query)
    return queryResultsDF
    
  }

}