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

object CCDMSReportGenerator extends Serializable {

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  var ccdmsFinalDF: DataFrame = null;

  def main(args: Array[String]) {

    val startDate = args(0)
    val endDate = args(1)
    val tableName = args(3)

    val spark = SparkSession.builder().appName("CCDMSReportGenerator").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()

    logger.info("************ CCDMSReportGenerator input Params, startDate:" + startDate + " endDate: " + endDate + " tableName: " + tableName)
    val ccdmsDataDF: DataFrame = CommonUtils.extractData(spark, tableName, startDate, endDate)

    Try {
      ccdmsFinalDF = CreditCardTransfromUtils.transfromCreditCradData(spark, ccdmsDataDF).persist(StorageLevel.DISK_ONLY)
      ccdmsFinalDF.write.mode("Overwrite").saveAsTable("project.temp_ccdms_transform_data")
      ccdmsFinalDF.unpersist()

    } match {
      case Success(s) => logger.info("******** CCDMSReportGenerator.transfromCreditCradData: Transformations are successfully done on ccdms credit/debit data ********")

      case Failure(f) => {
        logger.error(" CCDMSReportGenerator.transfromCreditCradData: Error while doing Transformation on ccdms credit/debit data : " + f.getMessage)
        throw new Exception(f.getMessage)

      }
    }
  }
}