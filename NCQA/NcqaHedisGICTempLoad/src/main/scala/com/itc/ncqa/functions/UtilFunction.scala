package com.itc.ncqa.functions

import com.itc.ncqa.constants.NcqaGictlConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{datediff, lit, to_date}
import org.apache.spark.sql.types.DateType

object UtilFunction {


  /**
    *
    * @param spark - SparkSessionObject
    * @param hedisDf - fact_hedis_gaps_in_care df with dobsk
    * @param dimDateDf - dimdate table data
    * @param year - year
    * @return - dataframe which is added by age column
    * @usecase - This function is used to add age column to the hedis_gaps_in_care table
    */
  def ageColumnAddingFunction(spark:SparkSession, hedisDf:DataFrame, dimDateDf:DataFrame, year:String):DataFrame = {


    import spark.implicits._
    val dobDateValAddedDf = hedisDf.as("df1").join(dimDateDf.as("df2"), hedisDf.col(NcqaGictlConstants.dobskColame) === dimDateDf.col(NcqaGictlConstants.dateSkColName), NcqaGictlConstants.innerJoinType)
                                                    .select("df1.*","df2.calendar_date").withColumnRenamed(NcqaGictlConstants.calenderDateColName, "dob_temp").drop(NcqaGictlConstants.dobskColame)

    val resultantDf = dobDateValAddedDf.withColumn(NcqaGictlConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy"))
    val current_date = year + "-12-31"
    val newDf1 = resultantDf.withColumn("curr_date", lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))
    val ageColumnAddedDf = newDf2.withColumn(NcqaGictlConstants.ageColName,(datediff(newDf2.col("curr_date"), newDf2.col(NcqaGictlConstants.dobColName)) / 365.25))
    ageColumnAddedDf
  }

  /**
    *
    * @param spark
    * @param hedisDf
    * @param factMembershipDf
    * @param refLobDf
    * @return
    */
  def lobDetailsAddingFunction(spark:SparkSession , hedisDf:DataFrame , factMembershipDf:DataFrame,refLobDf:DataFrame):DataFrame ={

    val joinedForLobDetailsDf = hedisDf.as("df1").join(factMembershipDf.as("df2"),hedisDf.col(NcqaGictlConstants.productPlanSkColName) === factMembershipDf.col(NcqaGictlConstants.productPlanSkColName),NcqaGictlConstants.innerJoinType)
                                                        .join(refLobDf.as("df3"),factMembershipDf.col(NcqaGictlConstants.lobIdColName) === refLobDf.col(NcqaGictlConstants.lobIdColName),NcqaGictlConstants.innerJoinType)
                                                        .select("df1.*","df3.lob","df3.lob_name","df3.lob_desc")

    joinedForLobDetailsDf

  }
}
