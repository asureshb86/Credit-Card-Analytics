package com.wfs.ccdms.analytics.utils

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.collection.Map
import java.time.LocalTime

object CreditCardTransfromUtils {

  def transfromCreditCradData(spark: SparkSession, ccdmsStagingDF: DataFrame, startDate: String, endDate: String, transactionCategoryMap : Map[String, String] ): DataFrame = {
    val decimalType = DataTypes.createDecimalType(10, 3)

    val schema = StructType(Seq(StructField("customer_name", StringType), StructField("transcation_category", StringType),
      StructField("amount_spent", DoubleType), StructField("category_count", StringType), StructField("trans_date_time", StringType),
      StructField("rewards_points_accured", IntegerType),StructField("date_range",StringType),StructField("report_gen_date",StringType)))

    val encoder = RowEncoder(schema)
    val ccmdsDF = ccdmsStagingDF.repartition(200).mapPartitions(partition =>
      {
        val newPartitions = partition.map(
          row => {

            var rewardPoints, totalCategoryCount = 0
            var rewardPointsAccured = 0.0
           
            val transId = row.getAs[Int]("trans_id")
            val customerName = row.getAs[String]("First_name") + " " + row.getAs[String]("last_name")
            val transcationcategory = row.getAs[String]("purchase_category")
            val amountSpent = row.getAs[java.math.BigDecimal]("trans_amt").doubleValue
            val cardNumber = row.getAs[java.math.BigInteger]("card_num").toString()
            val transDateTime = row.getAs[String]("trans_date_trans_time")
            val reportGenDate =  LocalTime.now().toString()

            /* Assign rewards points based on cardNumber - starts */
            if (cardNumber.startsWith("3"))
              rewardPoints = 4
            else if (cardNumber.startsWith("4"))
              rewardPoints = 4
            else if (cardNumber.startsWith("2"))
              rewardPoints = 4
            else if (cardNumber.startsWith("6"))
              rewardPoints = 4
            else
              rewardPoints = 1
            /* Assign rewards points based on cardNumber - Ends */
              
            /* Calculate reward Points for each and every transaction - Starts */  
            if (amountSpent >= 100)
              rewardPointsAccured = (amountSpent / 100) * rewardPoints
            /* Calculate reward Points for each and every transaction - Ends */
              
            /* Calculate total number of purchase category  - starts */   
            if (transcationcategory.equals("personal_care"))
              totalCategoryCount = ccdmsReportUtils.getTranscationCategoryCount(transactionCategoryMap)._1
            else if (transcationcategory.equals("health"))
              totalCategoryCount = ccdmsReportUtils.getTranscationCategoryCount(transactionCategoryMap)._2
            else if (transcationcategory.equals("travel"))
              totalCategoryCount = ccdmsReportUtils.getTranscationCategoryCount(transactionCategoryMap)._3
            else if (transcationcategory.equals("food"))
              totalCategoryCount = ccdmsReportUtils.getTranscationCategoryCount(transactionCategoryMap)._4
            else if (transcationcategory.equals("shopping_net"))
              totalCategoryCount = ccdmsReportUtils.getTranscationCategoryCount(transactionCategoryMap)._5
            else
              totalCategoryCount = 0
              
             /* Calculate total number of purchase category  - Ends */ 
              
            val newRow = Row.fromSeq(Seq(transId, customerName, transcationcategory, amountSpent, totalCategoryCount, transDateTime, rewardPointsAccured, startDate + " : " +endDate, reportGenDate))
            newRow
          }).toList
        newPartitions.iterator
      })(encoder).toDF
    return ccmdsDF
  }
}