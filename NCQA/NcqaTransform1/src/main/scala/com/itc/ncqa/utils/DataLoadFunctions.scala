package com.itc.ncqa.utils

import com.itc.ncqa.constants.TransformConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoadFunctions {


  /**
    *
    * @param spark     (spark session object)
    * @param tableName (table name that we have to load to df)
    * @param kpiName   (kpi name for which we are going to load the data)
    * @return - (dataframe that contains the data from the table who has the given kpi name)
    * @usecase -(Function is used to load the data from intermediate table for a specific kpi)
    */
  def sourceTableLoadFunction(spark: SparkSession, tableName: String, kpiName: String): DataFrame = {

    val kpi_name = "'" + kpiName + "'"
    val query = "select * from " + TransformConstants.sourcedbName + "." + tableName + " where kpi_name =" + kpi_name
    val df = spark.sql(query)
    df
  }

  /**
    *
    * @param spark (spark session object)
    * @param tableName (table that we have to laod)
    * @return - dataframe which contains the data from the specified table
    * @usecase - this function is used to load the data from a target table
    */
  def targetTableLoadFunction(spark: SparkSession,tableName: String):DataFrame ={

    val df = spark.sql("select * from "+ TransformConstants.targetDbName + "."+ tableName)
    df
  }

  /**
    *
    * @param spark (SparkSession Object)
    * @return dataframe that contains the date_sk and calender_date
    * @usecase This function is used to load the date_sk and calender_date from the dim_date table.
    */
  def dimDateLoadFunction(spark: SparkSession): DataFrame = {
    val sqlQuery = "select date_sk, calendar_date from " + TransformConstants.targetDbName + "." + TransformConstants.dimDateTableName
    val dimDateDf = spark.sql(sqlQuery)
    dimDateDf
  }


}
