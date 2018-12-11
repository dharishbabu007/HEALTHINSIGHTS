package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NcqaCAP3 {

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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaCAP1")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.capMeasureTitle)

    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age12Val, KpiConstants.age19Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /*Continous enrollment calculation*/
    val contEnrollStrtDate = year.toInt -1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continiousEnrollDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(ageFilterDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate)))

    /*Dinominator Calculation starts*/
    val dinominatorDf = continiousEnrollDf
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation ends*/


    /*Numerator calculation starts*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    /*Ambulatory Visit as Primary Diagnosis*/
    val ambulatoryVisitList = List(KpiConstants.ambulatoryVisitVal)
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForAmbVisAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.capMeasureId,ambulatoryVisitList,primaryDiagCodeSystem)
    val measurementForAmbVisAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForAmbVisAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Ambulatory Visit as Proceedure code*/
    val ambulatoryVisitCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAmbVisAsProDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.capMeasureId,ambulatoryVisitList,ambulatoryVisitCodeSystem)
    val measurementForAmbVisAsProDf = UtilFunctions.mesurementYearFilter(joinedForAmbVisAsProDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val ambulatoryVistDf = measurementForAmbVisAsDiagDf.union(measurementForAmbVisAsProDf)

    /*Numerator(members who has in dinominator and an ambulatory visit in the measurement year)*/
    val numeratorDf = ambulatoryVistDf.intersect(dinoForKpiCalDf)
    /*Numerator calculation ends*/
  }
}
