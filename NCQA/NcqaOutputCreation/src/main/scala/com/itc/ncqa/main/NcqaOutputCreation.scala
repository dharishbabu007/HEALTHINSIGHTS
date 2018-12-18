package com.itc.ncqa.main

import com.itc.ncqa.Functions.{DataLoadFunctions, UtilityFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NcqaOutputCreation {


  def main(args: Array[String]): Unit = {



    val qualityMsrString = args(0)
    val lobName = args(1)
    val ageLower = args(2)
    val ageUpper = args(3)





    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQOutputCreation")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()



    /*val qualityMsrTitle = UtilityFunctions.getQualityMeasureTitle(qualityMsrString)
    val qualityMsrSk = UtilityFunctions.getQualityMsrSk(spark,qualityMsrTitle)*/
    val gapsHedisTableDf = DataLoadFunctions.dataLoadFromGapsHedisTable(spark,qualityMsrString,lobName,ageLower.toInt,ageUpper.toInt)
    //gapsHedisTableDf.select("member_sk").orderBy("member_sk").show(50)
    val outputDf = UtilityFunctions.outputCreationFunction(spark,gapsHedisTableDf,qualityMsrString)
  }
}
