package com.itc.ncqa.functions

import com.itc.ncqa.constants.NcqaGictlConstants
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoadFunction {



  /**
    *
    * @param df           (input dataframe)
    * @param headervalues (column names of the input dataframe)
    * @param colName      (column name  )
    * @return Dataframe that removes the header if it presents
    */
  def removeHeaderFromDf(df: DataFrame, headervalues: Array[String], colName: String): DataFrame = {

    val df1 = df.filter(df.col(colName).isin(headervalues: _*))
    //df1.show()
    val returnDf = df.except(df1)
    returnDf
  }


  /**
    *
    * @param spark      (SparkSession Object)
    * @param dbName     (database name)
    * @param tblName    (table name)
    * @param sourceName (source name which get from the program argument)
    * @return dataframe that laods the data which has the source name that we have passed from the coreesponding hive table.
    * @usecase Function is used to laod the dim and afact tables from the targeted Hive table
    */
  def dataLoadFromTargetModel(spark: SparkSession, tblName: String, sourceName: String): DataFrame = {

    val source_name = "'" + sourceName + "'"
    val query = "select * from " + NcqaGictlConstants.db_name + "." + tblName + " where source_name =" + source_name
    val df_init = spark.sql(query)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  /**
    *
    * @param spark - spark session object
    * @param measureId - ncqa measure id
    * @return - dataframe contains the data from fact hedis gaps in care table
    * @usecase - This function is used to laod data from fact_hedis_gaps_in_care table based on the
    *            specified measureid.
    */
  def dataLoadDataFromGapsInCare(spark:SparkSession,measureId:String):DataFrame ={

    val measure_id = "'" + measureId + "'"
    val query = "select * from " + NcqaGictlConstants.db_name + "." + NcqaGictlConstants.factHedisGapsInCareTaleName + " where ncqa_measureid="+ measure_id
    val df = spark.sql(query)
    df
  }


  /**
    *
    * @param spark (SparkSession Object)
    * @return dataframe that contains the date_sk and calender_date
    * @usecase This function is used to load the date_sk and calender_date from the dim_date table.
    */
  def dimDateLoadFunction(spark:SparkSession):DataFrame ={
    val sqlQuery = "select date_sk, calendar_date from "+ NcqaGictlConstants.db_name +"."+ NcqaGictlConstants.dimDateTblName
    val dimDateDf = spark.sql(sqlQuery)
    dimDateDf
  }

}
