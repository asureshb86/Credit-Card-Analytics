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

object CreditCardTransfromUtils {

  def transfromCreditCradData(spark: SparkSession, ccdmsStagingDF: DataFrame): DataFrame = {
    val decimalType = DataTypes.createDecimalType(10, 3)

    val schema = StructType(Seq(StructField("customer_name", StringType), StructField("transcation_category", StringType),
      StructField("amount_spent", DoubleType), StructField("max_amount_spent_category", DoubleType),
      StructField("rewards_points_accrued", IntegerType), StructField("spent_redeem_points", StringType)))

    val encoder = RowEncoder(schema)
    val ccmdsDF = ccdmsStagingDF.repartition(200).mapPartitions(partition =>
      {
        val newPartitions = partition.map(
          row => {

            var rewardPoints = 0
            var rewardPointsAccured = 0.0

            val customerName = row.getAs[String]("First_name") + " " + row.getAs[String]("last_name")
            val transcationcategory = row.getAs[String]("purchase_category")
            val amountSpent = row.getAs[java.math.BigDecimal]("trans_amt").doubleValue
            val cardNumber = row.getAs[java.math.BigInteger]("card_num").toString()

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
              
              if(amountSpent > 100 )
                rewardPointsAccured = (amountSpent / 100 ) * 4
              
            val newRow = Row.fromSeq(Seq(customerName, transcationcategory, amountSpent, "", rewardPointsAccured))
            newRow
          }).toList
        newPartitions.iterator
      })(encoder).toDF
    return ccmdsDF
  }
}