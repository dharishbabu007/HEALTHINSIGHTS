package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoadFunctions {


  /**
    *
    * @param spark (SparkSession Object)
    * @param dbName (database name)
    * @param tblName (table name)
    * @param sourceName (source name which get from the program argument)
    * @return dataframe that laods the data which has the source name that we have passed from the coreesponding hive table.
    * @usecase Function is used to laod the dim and afact tables from the targeted Hive table
    */
  def dataLoadFromTargetModel(spark:SparkSession,dbName:String,tblName:String,sourceName:String):DataFrame ={

    val source_name = "'"+sourceName+"'"
    val query = "select * from "+ dbName+"."+ tblName+" where source_name ="+source_name
    val df_init = spark.sql(query)
    //df_init.select("source_name").show(50)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = UtilFunctions.removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  /**
    *
    * @param spark (SparkSession Object)
    * @param dbName (database name)
    * @param tblName (Table name)
    * @return Datfarme that conatins the data from thae corresponding Hive table
    * @usecase Function is used to load the ref tables.
    */
  def referDataLoadFromTragetModel(spark:SparkSession,dbName:String,tblName:String):DataFrame ={

    val query = "select * from "+ dbName+"."+ tblName
    val df_init = spark.sql(query)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = UtilFunctions.removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  /**
    *
    * @param spark (SparkSession Object)
    * @return dataframe that contains the date_sk and calender_date
    * @usecase This function is used to load the date_sk and calender_date from the dim_date table.
    */
  def dimDateLoadFunction(spark:SparkSession):DataFrame ={
    val dimDateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    dimDateDf
  }


  /**
    *
    * @param spark (SparkSession Object)
    * @param measureTitle (Measure title which get from the program argument)
    * @return Dataframe that contains the quality_measure_sk for the measure title
    * @usecase This functio ius used to find the quality_measure_sk for the measure title that passed as argument
    */
  def qualityMeasureLoadFunction(spark:SparkSession,measureTitle:String):DataFrame ={

    val measure_title = "'"+measureTitle+"'"
    val query = "select quality_measure_sk from "+ KpiConstants.dbName+"."+ KpiConstants.dimQltyMsrTblName+" where measure_title ="+measure_title
    val dimQualityMsrDf = spark.sql(query)
    dimQualityMsrDf
  }


  /**
    *
    * @param spark (SparkSession Object)
    * @param viewName (view name that has to call)
    * @return dataframe that contains the view data
    * @usecase This function is used to call the view name and loads the view data to dataframe
    */
  def viewLoadFunction(spark:SparkSession,viewName:String):DataFrame = {

    val newDf = viewName match {
        case KpiConstants.view45Days => spark.sql(KpiConstants.view45DaysLoadQuery)
        case KpiConstants.view60Days => spark.sql(KpiConstants.view60DaysLoadQuery)
    }
    newDf
  }


}
