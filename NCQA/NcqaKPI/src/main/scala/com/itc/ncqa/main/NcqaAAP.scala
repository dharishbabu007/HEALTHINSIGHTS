package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaAAP {


  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and SaprkSession oBject creation">

    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    val measureId = args(4)
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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAAAP")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Continuous Enrollment, Allowable Gap, Age filter.">

    /*Join dimMember,factclaim,factmembership,reflob,dimfacility,dimlocation.*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.aapMeasureTitle)


    /*Continous enrollment checking*/
    val contEnrollEndDate = year + "-12-31"
    var contEnrollStartDate = ""
    /*continous enrollment start date based on the lobName*/
    if(KpiConstants.medicaidLobName.equalsIgnoreCase(lob_name) || KpiConstants.medicareLobName.equalsIgnoreCase(lob_name)){

      contEnrollStartDate = year + "-01-01"
    }
    else{
      contEnrollStartDate = year.toInt-2 + "-01-01"
    }
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))





    /*Allowable gap filter Calculation */
    var lookUpDf = spark.emptyDataFrame
    if(KpiConstants.commercialLobName.equalsIgnoreCase(lob_name) || KpiConstants.medicareLobName.equalsIgnoreCase(lob_name)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }


    /*common filter checking*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"),continuousEnrollDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*Age filter based on the measure id*/
    var ageFilterDf = spark.emptyDataFrame

    /*Age 20–44 (Aap1 Activity)*/
    if(KpiConstants.aap1MeasureId.equalsIgnoreCase(measureId)){

      ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age20Val, KpiConstants.age44Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }
    /*Age 45–64 (Aap2 Activity)*/
    else if(KpiConstants.aap2MeasureId.equalsIgnoreCase(measureId)){

      ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age45Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }
    /*Age 65+ (Aap3 Activity)*/
    else {

      ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()
    val dinominatorExclusionDf = hospiceDf
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    //dinominatorAfterExclusionDf.show()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="Ambulatory valueset as  primaryDiagnosisCodeSystem">

    val ambulatoryVisistValList = List(KpiConstants.ambulatoryVisitVal)
    val primaryDiagnosisCodeVal = List(KpiConstants.icdCodeVal)
    var measurForAmbulVisistDf = spark.emptyDataFrame
    val joinedForAmbulVisistDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.aapMeasureId, ambulatoryVisistValList, primaryDiagnosisCodeVal)
    /*Measurement year filter based on the lob_name(for commercial(year and 2 years prior to the year))*/
    if(KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)){

      measurForAmbulVisistDf = UtilFunctions.measurementYearFilter(joinedForAmbulVisistDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement3Val).select(KpiConstants.memberskColName)
    }
    /*for medicare and medicaid measurement year.*/
    else{

      measurForAmbulVisistDf = UtilFunctions.measurementYearFilter(joinedForAmbulVisistDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    }
    //</editor-fold>

    //<editor-fold desc="Ambulatory visit,Other Ambulatory Visit, Telephone visit,Online Assesment as proceedureCodeSystem">

    val joinValList = List(KpiConstants.ambulatoryVisitVal, KpiConstants.otherAmbulatoryVal, KpiConstants.telephoneVisitsVal, KpiConstants.onlineAssesmentVal)
    val joinCodeVal = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.allInclusiveCodeVal, KpiConstants.ppsCodeVal, KpiConstants.initialCodeVal, KpiConstants.g0402CodeVal, KpiConstants.unspecifiedCodeVal)
    val joinForaapAsProDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.aapMeasureId, joinValList, joinCodeVal)
    var measurForaapAsProDf = spark.emptyDataFrame
    if(KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)){

      measurForaapAsProDf = UtilFunctions.measurementYearFilter(joinForaapAsProDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement3Val).select(KpiConstants.memberskColName)
    }
    else{

      measurForaapAsProDf = UtilFunctions.measurementYearFilter(joinForaapAsProDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    }
    //</editor-fold>

    val numeratorUnionDf = measurForAmbulVisistDf.union(measurForaapAsProDf)
    val numeratorDf = numeratorUnionDf.intersect(dinominatorAfterExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = joinValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }



}
