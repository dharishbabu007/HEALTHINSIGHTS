package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConversions._

object Test {


  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC2")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val programType = args(0)
    var data_source =""
    /*define data_source based on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }
    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables*/



    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)

    val qualityMeasureSk =  DataLoadFunctions.qualityMeasureLoadFunction(spark,KpiConstants.abaMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk","lob_id")
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark,factMembershipDfForoutDf,qualityMeasureSk,data_source)
    outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
  }
}
