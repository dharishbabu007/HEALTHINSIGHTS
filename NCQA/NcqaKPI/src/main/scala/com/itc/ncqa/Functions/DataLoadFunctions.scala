package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoadFunctions {





  def dataLoadFromTargetModel(spark:SparkSession,dbName:String,tblName:String,sourceName:String):DataFrame ={

    val source_name = "'"+sourceName+"'"
    val query = "select * from "+ dbName+"."+ tblName+" where source_name ="+source_name
    val df_init = spark.sql(query)
    //df_init.select("source_name").show(50)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = UtilFunctions.removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  def referDataLoadFromTragetModel(spark:SparkSession,dbName:String,tblName:String):DataFrame ={

    val query = "select * from "+ dbName+"."+ tblName
    val df_init = spark.sql(query)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = UtilFunctions.removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  def dimDateLoadFunction(spark:SparkSession):DataFrame ={
    val dimDateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    dimDateDf
  }


  def qualityMeasureLoadFunction(spark:SparkSession,measureTitle:String):DataFrame ={

    val measure_title = "'"+measureTitle+"'"
    val query = "select quality_measure_sk from "+ KpiConstants.dbName+"."+ KpiConstants.dimQltyMsrTblName+" where measure_title ="+measure_title
    val dimQualityMsrDf = spark.sql(query)
    dimQualityMsrDf
  }


  def viewLoadFunction(spark:SparkSession,viewName:String):DataFrame = {

    val newDf = viewName match {
        case KpiConstants.view45Days => spark.sql(KpiConstants.view45DaysLoadQuery)
        case KpiConstants.view60Days => spark.sql(KpiConstants.view60DaysLoadQuery)
    }
    newDf
  }


}
