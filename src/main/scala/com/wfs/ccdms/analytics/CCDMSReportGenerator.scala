package com.wfs.ccdms.analytics

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.wfs.ccdms.analytics.utils.CommonUtils
import scala.util.Try
import com.wfs.ccdms.analytics.utils.CreditCardTransfromUtils
import org.apache.spark.storage.StorageLevel
import scala.util.Success
import scala.util.Failure
import com.wfs.ccdms.analytics.utils.ccdmsReportUtils

object CCDMSReportGenerator extends Serializable {

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  var ccdmsFinalDF: DataFrame = null;

  def main(args: Array[String]) {

    val startDate = args(0)
    val endDate = args(1)
    val firstName = args(2)
    val lastName = args(3)
    val tableName = args(4)

    val spark = SparkSession.builder().appName("CCDMSReportGenerator").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
    import spark.implicits._
    
    logger.info("************ CCDMSReportGenerator input Params, startDate:" + startDate + " endDate: " + endDate + " tableName: " + tableName)
    val ccdmsDataDF: DataFrame = CommonUtils.extractData(spark, tableName, startDate, endDate, firstName, lastName)
    val trasactionCategoryDF = ccdmsReportUtils.getTransactionCategory(spark, tableName, startDate, endDate, firstName, lastName)
    val transcationCategoryMap = trasactionCategoryDF.as[(String, String)].collect.toMap
    logger.info("******* CCDMSReportGenerator: size of transcationCategoryMap:" +transcationCategoryMap.size)  
    
    Try {
      
      ccdmsFinalDF = CreditCardTransfromUtils.transfromCreditCradData(spark, ccdmsDataDF, startDate, endDate, transcationCategoryMap).persist(StorageLevel.DISK_ONLY)
      ccdmsFinalDF.write.mode("Overwrite").saveAsTable("ccdms_analysis.temp_customer_report")
      ccdmsFinalDF.unpersist()

    } match {
      case Success(s) => logger.info("******** CCDMSReportGenerator.transfromCreditCardData: Transformations are successfully done on ccdms credit/debit data ********")

      case Failure(f) => {
        logger.error(" CCDMSReportGenerator.transfromCreditCradData: Error while doing Transformation on ccdms credit/debit data : " + f.getMessage)
        throw new Exception(f.getMessage)

      }
    }
  }
}