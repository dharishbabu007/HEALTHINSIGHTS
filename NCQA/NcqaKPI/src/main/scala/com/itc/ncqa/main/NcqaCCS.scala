package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.commons.math3.analysis.function
import org.apache.commons.math3.analysis.function.Abs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import org.apache.commons.math3.analysis.function

object NcqaCCS {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACCS")
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

    //<editor-fold desc="Initila Join, Continuous Enrollment,Allowable gap and Age filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.ccsMeasureTitle)

    /*Continuous Enrollment*/
    var contEnrollStartDate = ""
    if(KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)){
      contEnrollStartDate = year.toInt - 2 + "-01-01"
    }
    else{
      contEnrollStartDate = year + "-01-01"
    }

    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))


    /*Allowable Gap filter*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")


    /*Age filter for dinominator*/
    val ageAndGenderFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age24Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).filter($"gender".===("F"))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = ageAndGenderFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    //<editor-fold desc="Dinominator Exclusion1(Absence od cervix)">

    /*Mmebers who has 'Absence of Cervix' as proceedure code*/
    val absOfCervixValList = List(KpiConstants.absOfCervixVal)
    val absOfCervixCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForabsOfCervixProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, absOfCervixValList, absOfCervixCodeSystem).select(KpiConstants.memberskColName)

    /*Mmebers who has 'Absence of Cervix' as primary diagnosis*/
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForabsOfCervixDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, absOfCervixValList, primaryDiagCodeSystem).select(KpiConstants.memberskColName)

    val absOfCervixDf = joinedForabsOfCervixProcDf.union(joinedForabsOfCervixDiagDf)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion2 (Hospice)">

    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    //</editor-fold>

    val dinominatorExclusionDf = absOfCervixDf.union(hospiceDf)
    val dinominatorAfterExclusionDf = dinominatorDf.except(dinominatorExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="Step 1(Cervical Cytology)">

    val cervicalCytologyValList = List(KpiConstants.cervicalCytologyVal)
    val cervicalCytologyCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.loincCodeVal)
    val joinedForcervCytoDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, cervicalCytologyValList, cervicalCytologyCodeSystem)
    val measurForcervCytoDf = UtilFunctions.mesurementYearFilter(joinedForcervCytoDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurement3Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val numeratorStep1Df = measurForcervCytoDf.select(KpiConstants.memberskColName).intersect(ageAndGenderFilterDf.select(KpiConstants.memberskColName))
    //</editor-fold>

    //<editor-fold desc="Step 2 (HPV test and Cervic test, not present in step1 and age greater than 30)">

    val age30to64Df = UtilFunctions.ageFilter(ageAndGenderFilterDf,KpiConstants.dobColName,year,KpiConstants.age30Val, KpiConstants.age64Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal).select(KpiConstants.memberskColName)
    /*Members who are not present in numeratorStep1Df and age is 30-64*/
    val memberDf = age30to64Df.except(numeratorStep1Df)

    val hpvtestValList = List(KpiConstants.hpvTestVal)
    val hpvtestCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.loincCodeVal)
    val joinedForhpvtestDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId,hpvtestValList, hpvtestCodeSsytem)
    val measurForhpvtestDf = UtilFunctions.measurementYearFilter(joinedForhpvtestDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Members who has both cervic and HPV test with service date aparts 4 or less days*/
    val cerAndhpvtestDf = measurForcervCytoDf.as("df1").join(measurForhpvtestDf.as("df2"),$"sdf1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                              .filter(abs(datediff($"sdf1.${KpiConstants.startDateColName}", $"sdf2.${KpiConstants.startDateColName}")).<=(4)).select($"sdf1.${KpiConstants.memberskColName}")
    //</editor-fold>

    val numeratorUnionDf = numeratorStep1Df.union(cerAndhpvtestDf)
    val numeratorDf = numeratorUnionDf.intersect(dinominatorAfterExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorReasonValueSet = hpvtestValList ::: cervicalCytologyValList
    val dinoExclReasonValueSet = absOfCervixValList
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cisDtpaMeasureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }

}
