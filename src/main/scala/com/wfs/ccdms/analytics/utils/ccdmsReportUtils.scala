package com.wfs.ccdms.analytics.utils

import scala.collection.Map
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object ccdmsReportUtils {

  def getTransactionCategory(spark: SparkSession, tableName: String, startDate: String, endDate: String, firstName: String, lastName: String): DataFrame = {

    val transCategoryDF: DataFrame = spark.sql("select CAST(trans_id AS varchar(13)) AS trans_id, purchase_category from " + tableName + " where businessdate >= " + startDate + " and businessdate <= " + endDate + " and first_name = " + firstName + " and last_name = " + lastName)
    transCategoryDF
  }
  
  def getTransAmountSpentOnCategory(spark: SparkSession, tableName: String, startDate: String, endDate: String, firstName: String, lastName: String): DataFrame = {

    val transCategoryDF: DataFrame = spark.sql("select purchase_category, trans_amt from " + tableName + " where businessdate >= " + startDate + " and businessdate <= " + endDate + " and first_name = " + firstName + " and last_name = " + lastName)
    transCategoryDF
  }
  
  def getTranscationCategoryCount(transactionCategoryMap: Map[String, String]): Tuple5[Int, Int, Int, Int, Int] = {

    var personalCareCount, healthCount, travelCount, foodCount, shoppingNetCount: Int = 0;

    transactionCategoryMap.foreach((trans_id) =>
      if (transactionCategoryMap.get(trans_id._1).toString().equalsIgnoreCase("personal_care"))
        personalCareCount = personalCareCount + 1
      else if (transactionCategoryMap.get(trans_id._1).toString().equalsIgnoreCase("health"))
        healthCount = healthCount + 1
      else if (transactionCategoryMap.get(trans_id._1).toString().equalsIgnoreCase("travel"))
        travelCount = travelCount + 1
      else if (transactionCategoryMap.get(trans_id._1).toString().equalsIgnoreCase("food"))
        foodCount = foodCount + 1
      else if (transactionCategoryMap.get(trans_id._1).toString().equalsIgnoreCase("shopping_net"))
        shoppingNetCount = shoppingNetCount + 1)

    var TransCategoryTuple: Tuple5[Int, Int, Int, Int, Int] = (personalCareCount, healthCount, travelCount, foodCount, shoppingNetCount)
    return TransCategoryTuple
    
  }
}