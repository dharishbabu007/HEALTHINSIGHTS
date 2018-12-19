package com.itc.ncqa.main

import com.itc.ncqa.constants.QmsLoadConstants
import com.itc.ncqa.util.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

object QmsLoad {

  def main(args: Array[String]): Unit = {


    val measureTitle = args(0)
    val dbName = args(1)
    val programType = args(2)
    QmsLoadConstants.setDbName(dbName)
    var data_source = ""

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = QmsLoadConstants.ncqaDataSource
    }
    else {
      data_source = QmsLoadConstants.clientDataSource
    }

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaQms Load")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    /*Loading qulaity measure sk from dim_quality_measure table using measure title.*/
    val qualityMeasureSk = UtilFunctions.qualityMeasureLoadFunction(spark, measureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDf = UtilFunctions.dataLoadFromTargetModel(spark, QmsLoadConstants.db_Name, QmsLoadConstants.factMembershipTblName, data_source).select("member_sk", "lob_id")

    /*calling function to generate the o/p format for fact_hedis_qms table.*/
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDf, qualityMeasureSk, data_source)
    outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(QmsLoadConstants.db_Name +"."+ QmsLoadConstants.factHedisQmsTableName)


  }
}
