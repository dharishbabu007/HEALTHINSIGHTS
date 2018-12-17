package com.itc.ncqa.util

import com.itc.ncqa.constants.QmsLoadConstants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object UtilFunctions {


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
    * @param spark        (SparkSession Object)
    * @param measureTitle (Measure title which get from the program argument)
    * @return Dataframe that contains the quality_measure_sk for the measure title
    * @usecase This functio ius used to find the quality_measure_sk for the measure title that passed as argument
    */
  def qualityMeasureLoadFunction(spark: SparkSession, measureTitle: String): DataFrame = {

    val measure_title = "'" + measureTitle + "'"
    val query = "select quality_measure_sk from " + QmsLoadConstants.db_Name + "." + QmsLoadConstants.dimQltyMsrTblName + " where measure_title =" + measure_title
    val dimQualityMsrDf = spark.sql(query)
    dimQualityMsrDf
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
  def dataLoadFromTargetModel(spark: SparkSession, dbName: String, tblName: String, sourceName: String): DataFrame = {

    val source_name = "'" + sourceName + "'"
    val query = "select * from " + dbName + "." + tblName + " where source_name =" + source_name
    val df_init = spark.sql(query)
    val dfColumns = df_init.columns.map(f => f.toUpperCase)
    val resultDf = removeHeaderFromDf(df_init, dfColumns, dfColumns(0))
    resultDf
  }


  /**
    *
    * @param spark            (SparkSession Object)
    * @param factMembershipDf (factmembership Dataframe)
    * @param qualityMeasureSk (quality measuresk)
    * @param sourceName       (source name which is calculate from the program argument)
    * @return Dataframe that contains the data required to populate in fact_hedis_qms table in hive
    * @usecase function loads the data from fact_gaps_in_hedis table and do the treansformation, add new columns and audit columns
    *          and return in the format that the data to be populated to the fact_hedis_qms table
    */
  def outputCreationForHedisQmsTable(spark: SparkSession, factMembershipDf: DataFrame, qualityMeasureSk: String, sourceName: String): DataFrame = {

    val quality_Msr_Sk = "'" + qualityMeasureSk + "'"

    /*Loading needed data from fact_gaps_in_hedis table*/
    val hedisDataDfLoadQuery = "select " + QmsLoadConstants.memberskColName + "," + QmsLoadConstants.outQualityMeasureSkColName + "," + QmsLoadConstants.outDateSkColName + "," + QmsLoadConstants.outProductPlanSkColName + "," + QmsLoadConstants.outFacilitySkColName + "," + QmsLoadConstants.outInNumColName + "," + QmsLoadConstants.outInDinoColName + "," + QmsLoadConstants.outInDinoExclColName + "," + QmsLoadConstants.outInNumExclColName + "," + QmsLoadConstants.outInDinoExcColName + "," + QmsLoadConstants.outInNumExcColName + " from ncqa_sample.fact_hedis_gaps_in_care where quality_measure_sk =" + quality_Msr_Sk
    val hedisDataDf = spark.sql(hedisDataDfLoadQuery)
    //hedisDataDf.show()
    /*join the hedisDataDf with factmembership data for getting the lob_id */
    val lobIdColAddedDf = hedisDataDf.as("df1").join(factMembershipDf.as("df2"), hedisDataDf.col(QmsLoadConstants.memberskColName) === factMembershipDf.col(QmsLoadConstants.memberskColName), QmsLoadConstants.innerJoinType).select("df1.*", "df2.lob_id")
    //lobIdColAddedDf.printSchema()

    /*group by the df using the columns (qualitymeasuresk,datesk,productplansk,facilitysk and lob_id),calculate the required aggregation and add those as new columns*/
    val groupByDf = lobIdColAddedDf.groupBy(QmsLoadConstants.qualityMsrSkColName, QmsLoadConstants.dateSkColName, QmsLoadConstants.outProductPlanSkColName, QmsLoadConstants.facilitySkColName, "lob_id").agg(count(when(lobIdColAddedDf.col(QmsLoadConstants.outInDinoColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsDinominatorColName),
      count(when(lobIdColAddedDf.col(QmsLoadConstants.outInNumColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsNumeratorColName),
      count(when(lobIdColAddedDf.col(QmsLoadConstants.outInNumExclColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsNumExclColName),
      count(when(lobIdColAddedDf.col(QmsLoadConstants.outInDinoExclColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsDinoExclColName),
      count(when(lobIdColAddedDf.col(QmsLoadConstants.outInNumExcColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsNumExcColName),
      count(when(lobIdColAddedDf.col(QmsLoadConstants.outInDinoExcColName).===(QmsLoadConstants.yesVal), true)).alias(QmsLoadConstants.outHedisQmsDinoExcColName))


    /*adding ratio and Bonus columns*/
    val ratioAndBonusColAddedDf = groupByDf.withColumn(QmsLoadConstants.outHedisQmsRatioColName, (groupByDf.col(QmsLoadConstants.outHedisQmsNumeratorColName)./(groupByDf.col(QmsLoadConstants.outHedisQmsDinominatorColName))).*(100)).withColumn(QmsLoadConstants.outHedisQmsBonusColName, lit(0)).withColumn(QmsLoadConstants.outHedisQmsPerformanceColName, lit(0))

    /*Adding the audit columns*/
    val auditColumnsAddedDf = ratioAndBonusColAddedDf.withColumn(QmsLoadConstants.outCurrFlagColName, lit(QmsLoadConstants.yesVal))
      .withColumn(QmsLoadConstants.outActiveFlagColName, lit(QmsLoadConstants.actFlgVal))
      .withColumn(QmsLoadConstants.outLatestFlagColName, lit(QmsLoadConstants.yesVal))
      .withColumn(QmsLoadConstants.outSourceNameColName, lit(sourceName))
      .withColumn(QmsLoadConstants.outUserColName, lit(QmsLoadConstants.userNameVal))
      .withColumn(QmsLoadConstants.outRecCreateDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(QmsLoadConstants.outRecUpdateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(QmsLoadConstants.outIngestionDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))


    /*Create and add the qms sk column based on the existing column values*/
    val outHedisQmsSkColAddedDf = auditColumnsAddedDf.withColumn(QmsLoadConstants.outHedisQmsSkColName, abs(hash(lit(concat(lit(auditColumnsAddedDf.col(QmsLoadConstants.outQualityMeasureSkColName)), lit(auditColumnsAddedDf.col(QmsLoadConstants.outProductPlanSkColName)), lit(auditColumnsAddedDf.col(QmsLoadConstants.outHedisQmsLobidColName)), lit(auditColumnsAddedDf.col(QmsLoadConstants.outFacilitySkColName)), lit(auditColumnsAddedDf.col(QmsLoadConstants.outDateSkColName)))))))

    /*selecting the data in the order of columns as same as in the fact_hedis_qms table in hive*/
    val outFormattedDf = outHedisQmsSkColAddedDf.select(QmsLoadConstants.outFactHedisQmsFormattedList.head, QmsLoadConstants.outFactHedisQmsFormattedList.tail: _*)
    //outFormattedDf.printSchema()
    outFormattedDf
  }
}
