package com.wfs.ccdms.analytics.utils

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object CommonUtils {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def extractData(spark: SparkSession, tableName: String, startDate: String, endDate: String): DataFrame = {

    var query = "select * from " + tableName + " where businessdate >= " + startDate + " and businessdate <= " + endDate;
    val queryResultsDF = spark.sql(query)
    return queryResultsDF
    
  }

}