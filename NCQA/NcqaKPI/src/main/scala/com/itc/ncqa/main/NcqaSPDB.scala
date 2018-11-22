package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NcqaSPDB {

  def main(args: Array[String]): Unit = {



    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source = ""

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASPDB")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    import spark.implicits._


    var lookupTableDf = spark.emptyDataFrame


    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)


    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spdMeasureTitle)

    /*Loading view table based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 40 and 75*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)



    /*Dinominator Calculation Starts*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val joinedForHmismDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"medication_list".isin(KpiConstants.spdHmismMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk","df2.end_date_sk","df3.medication_list")
    val startDateValAddedDfForHmismDf = joinedForHmismDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val endDateValAddedForHmismDf = startDateValAddedDfForHmismDf.as("df1").join(dimdateDf.as("df2"), $"df1.end_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "end_temp").drop("end_date_sk")
    val dateTypeDfForHmismDf = endDateValAddedForHmismDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).withColumn("end_date", to_date($"end_temp", "dd-MMM-yyyy")).drop("start_temp","end_temp")
    val MeasurementForHmismDf = UtilFunctions.mesurementYearFilter(dateTypeDfForHmismDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val dinoDf = MeasurementForHmismDf.select(KpiConstants.memberskColName)
    val dinominatorDf = ageFilterDf.as("df1").join(dinoDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinoDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation Ends*/


    /*Dinominator Exclusion starts*/

    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*Hospice Exclusion*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)

    /*Dinominator Exclusion ends*/


    /*Numerator Calculation starts*/
    //MeasurementForHmismDf.printSchema()
    val ipsdDf = MeasurementForHmismDf.select("*").groupBy(KpiConstants.memberskColName).agg(min(MeasurementForHmismDf.col(KpiConstants.startDateColName)).alias(KpiConstants.ipsdDateColName))
    //ipsdDf.printSchema()
    val ipsdAddedHmismDf = MeasurementForHmismDf.as("df1").join(ipsdDf.as("df2"),MeasurementForHmismDf.col(KpiConstants.memberskColName) === ipsdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*",KpiConstants.ipsdDateColName)
    //ipsdAddedHmismDf.printSchema()

    var current_date = year + "-12-31"
    val currDateAddedDf = ipsdAddedHmismDf.withColumn("curr", lit(current_date))
    val 

    /*Numerator Calculation ends*/
  }
}
