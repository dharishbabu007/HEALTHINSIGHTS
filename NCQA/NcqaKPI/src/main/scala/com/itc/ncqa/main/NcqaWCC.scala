package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object NcqaWCC {


  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and spark session Object creation">

    /*Reading the program arguments*/
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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAWCC")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.wccMeasureTitle)

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*Age filter based on the measure id*/
    var ageFilterDf = spark.emptyDataFrame

    /*Age 3–11 */
    if (KpiConstants.wcc1aMeasureId.equalsIgnoreCase(measureId) || KpiConstants.wcc1bMeasureId.equalsIgnoreCase(measureId) || KpiConstants.wcc1cMeasureId.equalsIgnoreCase(measureId)) {

      ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age3Val, KpiConstants.age11Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }
    /*Age 12–17 */
    else if (KpiConstants.wcc2aMeasureId.equalsIgnoreCase(measureId) || KpiConstants.wcc2bMeasureId.equalsIgnoreCase(measureId) || KpiConstants.wcc2cMeasureId.equalsIgnoreCase(measureId)) {

      ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age12Val, KpiConstants.age17Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }

    /*Continous enrollment checking*/
    val contEnrollEndDate = year + "-12-31"
    val contEnrollStartDate = year + "-01-01"
    val contEnrollDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && ageFilterDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    /*An outpatient visit (Outpatient Value Set) with a PCP or an OB/GYN during the measurement year */

    val outPatientVal = List(KpiConstants.outPatientVal)
    val outPatientCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val joinForOutPatientDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.measureIdColName, outPatientVal, outPatientCodeSystem).select(KpiConstants.memberskColName)
    val measrWccOutPatDf = UtilFunctions.measurementYearFilter(joinForOutPatientDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)

    val wccPcpObJoinDf = dimProviderDf.as("df1").join(measrWccOutPatDf.as("df2"), ($"df1.$KpiConstants.providerSkColName" === $"df2.$KpiConstants.providerSkColName"), joinType = KpiConstants.innerJoinType)
      .filter(($"df1.KpiConstants.pcpColName" === KpiConstants.yesVal) || ($"df1.KpiConstants.obgynColName" === KpiConstants.yesVal))
      .select("df2.KpiConstants.memberskColName")

    val dinominatorDf = contEnrollDf.as("df1").join(wccPcpObJoinDf.as("df2"), contEnrollDf.col(KpiConstants.memberskColName) === wccPcpObJoinDf.col(KpiConstants.memberskColName)).select("df1.*")
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /* Pregnancy Value Set exclusion for Females */

    val wccPregnancyVal = List(KpiConstants.pregnancyVal)
    val joinForPregnancyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccPregnancyVal, primaryDiagnosisCodeSystem)
    val measrPregnancyDf = UtilFunctions.measurementYearFilter(joinForPregnancyDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).filter($"gender".===("F"))

    val wccPregnancyDf = measrPregnancyDf.select(KpiConstants.memberskColName).distinct()

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()
    val dinominatorExclDf = hospiceDf.union(wccPregnancyDf)
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //dinominatorAfterExclusionDf.show()

    //</editor-fold>
/*
    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="BMI Percentile">

    /*Numerator Calculation (BMI Percentile Value Set) */
    val wccBmiVal = List(KpiConstants.bmiPercentileVal)
    val joinForWccBmiDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccBmiVal, primaryDiagnosisCodeSystem)
    val measrWccBmiDf = UtilFunctions.measurementYearFilter(joinForWccBmiDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Nutrition Counseling">

    /*Numerator1 Calculation (Nutrition Counseling  Value Set ) for primaryDiagnosisCodeSystem */

    val wccNutrinCounVal = List(KpiConstants.nutritionCounselVal)
    val wccNutrinCounCode = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinNutrinCounDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccNutrinCounVal, wccNutrinCounCode)
    val measrNutrinCounDf = UtilFunctions.measurementYearFilter(joinNutrinCounDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /*Numerator1 Calculation (Nutrition Counseling  Value Set ) for primaryDiagnosisCodeSystem */

    val joinNutrinCounDiaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccNutrinCounVal, primaryDiagnosisCodeSystem)
    val measrNutrinCounDiaDf = UtilFunctions.measurementYearFilter(joinNutrinCounDiaDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    val unionNutrinCounDf = measrNutrinCounDf.union(measrNutrinCounDiaDf)
    //</editor-fold>

    //<editor-fold desc="Physical Activity Counseling">

    /*Numerator1 Calculation (Physical Activity Counseling  Value Set ) for primaryDiagnosisCodeSystem */

    val wccPhysicalActVal = List(KpiConstants.physicalActCounselVal)
    val wccPhysicalActCode = List(KpiConstants.hcpsCodeVal)
    val joinPhysicalActDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccPhysicalActVal, wccPhysicalActCode)
    val measrPhysicalActDf = UtilFunctions.measurementYearFilter(joinNutrinCounDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /*Numerator1 Calculation (Physical Activity Counseling  Value Set ) for primaryDiagnosisCodeSystem */

    val joinPhysicalActDiaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.measureIdColName, wccPhysicalActVal, primaryDiagnosisCodeSystem)
    val measrPhysicalActDiaDf = UtilFunctions.measurementYearFilter(joinNutrinCounDiaDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    val unionPhysicalActDf = measrPhysicalActDf.union(measrPhysicalActDiaDf)

    //</editor-fold>


    /*find out the member_sk based on the measure id*/

    var wccNumeratorVal =  KpiConstants.emptyList
    val wccNumeratorDf = spark.emptyDataFrame

    measureId match {

      case KpiConstants.wcc1aMeasureId => measrWccBmiDf
        wccNumeratorVal = wccBmiVal
      case KpiConstants.wcc2aMeasureId => measrWccBmiDf
        wccNumeratorVal = wccBmiVal
      case KpiConstants.wcc1bMeasureId => unionNutrinCounDf
        wccNumeratorVal = wccNutrinCounVal
      case KpiConstants.wcc2bMeasureId => unionNutrinCounDf
        wccNumeratorVal = wccNutrinCounVal
      case KpiConstants.wcc1cMeasureId => unionPhysicalActDf
        wccNumeratorVal = wccPhysicalActVal
      case KpiConstants.wcc2cMeasureId => unionPhysicalActDf
        wccNumeratorVal = wccPhysicalActVal
    }

    val numeratorDf =  wccNumeratorDf.intersect(dinominatorAfterExclusionDf)
    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = wccNumeratorVal
    val dinominatorExclValueSet = wccPregnancyVal
    val numeratorExclValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
*/
  }
}
