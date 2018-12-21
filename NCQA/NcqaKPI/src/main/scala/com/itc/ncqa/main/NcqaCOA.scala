package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, dayofyear, expr, lit, to_date, when}

object NcqaCOA {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACOA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
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

    KpiConstants.setDbName(dbName)

    import spark.implicits._


    var lookupTableDf = spark.emptyDataFrame

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)

    /*Join dimMember,Factclaim,FactMembership,RefLocation,DimFacilty for initail filter*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.coaMeasureTitle)

    /*call the view based on the lob_name*/
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {
      lookupTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {
      lookupTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the element who is present in the 45 or 60 days view*/
    val commonFilterDf = joinedForInitialFilterDf.as("df1").join(lookupTableDf.as("df2"), joinedForInitialFilterDf.col(KpiConstants.memberskColName) === lookupTableDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookupTableDf.col("start_date").isNull).select("df1.*")

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Dinominator for output format (age between 66 and older)*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age66Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /* dinominator calculation starts */

    val dinominatorDf = ageFilterDf.select(KpiConstants.memberskColName).distinct()

    /*Dinominator Exclusion*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf)
    val measurementDinominatorExclDf = UtilFunctions.mesurementYearFilter(hospiceDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*COA dinominator for kpi calculation*/
    val coaDinominatorForKpiCal = dinominatorDf.except(measurementDinominatorExclDf)

    /* End of dinominator calculation */

    /* Numerator calculation */

    /*Numerator1 (Advance Care Planning Value Set during the measurement year) as procedure code*/
    val hedisJoinedForAdvanceCareDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaAdvanceCareValueSet, KpiConstants.coaAdvanceCareCodeSystem)

    /*Numerator1 (Advance Care Planning Value Set during the measurement year) as primary diagnosis*/
    val hedisJoinedForAdvanceCareIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaAdvanceCareValueSet, KpiConstants.primaryDiagnosisCodeSystem)

    val hedisJoinedForBothNumerator1Df = hedisJoinedForAdvanceCareDf.union(hedisJoinedForAdvanceCareIcdDf)

    val numerator1Df = UtilFunctions.mesurementYearFilter(hedisJoinedForBothNumerator1Df, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator2a (Medication Review Value Set during the measurement year) as procedure code*/
    val hedisJoinedForMedicationReviewDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaMedicationReviewValueSet, KpiConstants.coaMedicationReviewCodeSystem)
    val measurementFornumerator2aDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMedicationReviewDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)

    /*Numerator2b (Medication List Value Set during the measurement year) as procedure code*/
    val hedisJoinedForMedicationListDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaMedicationListValueSet, KpiConstants.coaMedicationListCodeSystem)
    val measurementFornumerator2bDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMedicationListDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)


    /*	Both of the following during the same visit */

    val numerator2a2bSameDateDf = hedisJoinedForMedicationReviewDf.as("df1").join(hedisJoinedForMedicationListDf.as("df2"), ($"df1.start_date" === $"df2.start_date"), KpiConstants.innerJoinType).select("df1.member_sk").distinct()

    /* Join with fact claims to get provider_sk */

    val numerator2a2bWithProviderSkDf = numerator2a2bSameDateDf.as("df1").join(factClaimDf.as("df2"), ($"df1.member_sk" === $"df2.member_sk"), KpiConstants.innerJoinType).select("df2.*")
    /* provider type is a prescribing practitioner or clinical pharmacist */

    val numerator2a2bSameProviderTypeDf = dimProviderDf.as("df1").join(numerator2a2bWithProviderSkDf.as("df2"), ($"df1.provider_sk" === $"df2.provider_sk"), KpiConstants.innerJoinType).filter(($"df1.provider_prescribing_privileges" === KpiConstants.yesVal) || ($"df1.clinical_pharmacist" === KpiConstants.yesVal)).select("df2.member_sk").distinct()

    /* Numerator2c Transitional Care Management Services Value Set */

    val hedisJoinedForTransitionalCareDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaTransitionalCareValueSet, KpiConstants.coaTransitionalCareCodeSystem)
    val measurementFornumerator2cDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTransitionalCareDf , "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /* Join Numerator1 & 2 & 3 */

    val finalNumerator2BeforeExcludeDf = numerator2a2bSameProviderTypeDf.union(measurementFornumerator2cDf)


    /* Numerator Exclude services provided in an acute inpatient setting */

    val hedisJoinedForNumeratorExcludeDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaNumeratorExcludeValueSet, KpiConstants.coaNumeratorExcludeCodeSystem)
    val measurementForNumeratorExcludeDf = UtilFunctions.mesurementYearFilter(hedisJoinedForNumeratorExcludeDf , "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    val finalNumerator2Df = finalNumerator2BeforeExcludeDf.except(measurementForNumeratorExcludeDf)

    /* Numerator3 for Functional Status Assessment */

    val hedisJoinedForFunctionalStatusDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaFunctionalStatusValueSet, KpiConstants.coaFunctionalStatusCodeSystem)
    val measurementForFunctionalStatusDf = UtilFunctions.mesurementYearFilter(hedisJoinedForFunctionalStatusDf , "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    val finalNumerator3Df = measurementForFunctionalStatusDf.except(measurementForNumeratorExcludeDf).select(KpiConstants.memberskColName).distinct()

    /* Numerator4 for Pain Assessment  */

    val hedisJoinedForPainAssessmentDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, KpiConstants.coaPainAssessmentValueSet, KpiConstants.coaPainAssessmentCodeSystem)
    val measurementForPainAssessmentDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPainAssessmentDf , "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    val finalNumerator4Df = measurementForPainAssessmentDf.except(measurementForNumeratorExcludeDf)

    /* Final dinominator union of all Numerators 1, 2, 3 and 4 */

    val numeratorDf = numerator1Df.union(finalNumerator2Df).union(finalNumerator3Df).union(finalNumerator4Df)

    /* numerator after dinominator exclusion */

    val coaNumeratorDf = numeratorDf.intersect(coaDinominatorForKpiCal)


    /*Common output creation starts(data to fact_gaps_in_hedis table)*/

    /*reason valueset */
    val numeratorValueSet = KpiConstants.coaAdvanceCareValueSet ::: KpiConstants.coaMedicationReviewValueSet ::: KpiConstants.coaMedicationListValueSet ::: KpiConstants.coaTransitionalCareValueSet ::: KpiConstants.coaFunctionalStatusValueSet ::: KpiConstants.coaPainAssessmentValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.coaNumeratorExcludeValueSet
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.coaMeasureId)

    /*create empty NumeratorExcldf*/
    val numExclDf = spark.emptyDataFrame
    val outFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, measurementDinominatorExclDf, coaNumeratorDf, measurementForNumeratorExcludeDf, outValueSetForOutput, sourceAndMsrList)
    //outFormattedDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)

    spark.sparkContext.stop()
  }


}