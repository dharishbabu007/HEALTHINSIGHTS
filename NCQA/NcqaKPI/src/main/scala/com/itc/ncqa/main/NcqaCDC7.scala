package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConversions._

object NcqaCDC7 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC7")
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

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)


    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)


    /*Join dimMember,factclaim,factmembership,reflob,dimfacility,dimlocation.*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cdcMeasureTitle)


    /*Load the look up view based on the lob_name*/
    var lookUpTableDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }


    /*Common filter (Removing the elements who has a gap of 45 days or 60 days)*/
    val commonFilterDf = joinedForInitialFilterDf.as("df1").join(lookUpTableDf.as("df2"), joinedForInitialFilterDf.col(KpiConstants.memberskColName) === lookUpTableDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter("start_date is null").select("df1.*")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //ageFilterDf.orderBy("member_sk").show(50)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)


    /*calculating Dinominator*/

    /*Dinominator First condition */
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdcDiabetesvalueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForFirstDino = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino, "start_date", year, 0, 730).select("member_sk").distinct()


    /*Dinominator Second Condition*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"measure_id".===(KpiConstants.cdcMeasureId)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()


    /*union of first and second dinominator condition*/
    val dinominatorUnionDf = measurementForFirstDino.union(MeasurementForSecondDinoDf)

    /*dinominator  for ouput Calculation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === dinominatorUnionDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.*")
    val dinoMemberSkDf = dinominatorDf.select(KpiConstants.memberskColName)
    //dinominatorDf.show(50)


    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()


    /*dinominator Exclusion 2*/
    val hedisJoinedForDiabetesExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdcDiabetesExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDiabetesExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName).distinct()

    /*Union of Dinominator Exclusion*/
    val unionDinominatorExclusionDf = hospiceDf.union(measurementDiabetesExclDf).distinct()
    val dinominatorExclusionDf = ageFilterDf.as("df1").join(unionDinominatorExclusionDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === unionDinominatorExclusionDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*Dinominator after Dinominator Exclusion*/
    val cdc7DinominatorForKpiCalDf = dinoMemberSkDf.except(dinominatorExclusionDf)


    /*Numerator1 Calculation (Nephropathy screening or monitoring test)*/
    val hedisJoinedForNephropathyScreeningDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7uptValueSet, KpiConstants.cdc7uptCodeSystem)
    val measurementForNephropathyScreeningDf = UtilFunctions.mesurementYearFilter(hedisJoinedForNephropathyScreeningDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator2 Calculation (Evidence of treatment for nephropathy or ACE/ARB therapy) as proceedure code*/
    val hedisJoinedForNephropathyTreatmentAsPrDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7NtValueSet, KpiConstants.cdc7NtCodeSystem)
    val measurementForNephropathyTreatmentAsPrDf = UtilFunctions.mesurementYearFilter(hedisJoinedForNephropathyTreatmentAsPrDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator2 Calculation (Evidence of treatment for nephropathy or ACE/ARB therapy) as primary diagnosis*/
    val hedisJoinedForNephropathyTreatmentAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7NtValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForNephropathyTreatmentAsDiagDf = UtilFunctions.mesurementYearFilter(hedisJoinedForNephropathyTreatmentAsDiagDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator2 Calculation (Union of  measurementForNephropathyTreatmentAsPrDf and measurementForNephropathyTreatmentAsDiagDf)*/
    val NephropathyTreatmentDf = measurementForNephropathyTreatmentAsPrDf.union(measurementForNephropathyTreatmentAsDiagDf)


    /*Numerator3 Calculation (Evidence of stage 4 chronic kidney disease)*/
    val hedisJoinedForCkdStage4Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3CkdStage4ValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForCkdStage4Df = UtilFunctions.mesurementYearFilter(hedisJoinedForCkdStage4Df, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator4 Calculation (Evidence of ESRD) as proceedure code*/
    val hedisJoinedForEsrdAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7EsrdValueSet, KpiConstants.cdc3EsrdExclcodeSystem)
    val measurementForEsrdAsProDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsProDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator4 Calculation (Evidence of ESRD) as primary Diagnosis*/
    val hedisJoinedForEsrdAsDaigDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7EsrdValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForEsrdAsDaigDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsDaigDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator4 (union of measurementForEsrdAsProDf and measurementForEsrdAsDaigDf)*/
    val esrdDf = measurementForEsrdAsProDf.union(measurementForEsrdAsDaigDf)


    /*Numerator5 ,Evidence of kidney transplant as proceedure code */
    val hedisJoinedForKtAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7KtValueSet, KpiConstants.cdc7KtCodeSystem)
    val measurementForKtAsProDf = UtilFunctions.mesurementYearFilter(hedisJoinedForKtAsProDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator5 ,Evidence of kidney transplant as primary Diagnosis */
    val hedisJoinedForKtAsDaigDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7KtValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForKtAsDaigDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsDaigDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator5 ,Evidence of kidney transplant(Union of measurementForKtAsProDf and measurementForKtAsDaigDf)*/
    val kidneyTranspalantDf = measurementForKtAsProDf.union(measurementForKtAsDaigDf).distinct()


    /*Numerator6 (visit with a nephrologist)*/
    val hedisJoinedForNephrologistDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForFirstNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val joinedWithDimProviderDf = hedisJoinedForNephrologistDf.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForNephrologistDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType).filter(dimProviderDf.col(KpiConstants.nephrologistColName).===("Y"))
    val measurementForNephrologistDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator 7 (At least one ACE inhibitor or ARB dispensing event)*/
    val medValuesetForAceInhibitorDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"measure_id".===("CDC") && ($"medication_list".===("ACE Inhibitor/ARB Medications"))).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedForAceInhibitorDf = medValuesetForAceInhibitorDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeForAceInhibitorDf = startDateValAddedForAceInhibitorDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForAceInhibitorDf = UtilFunctions.mesurementYearFilter(dateTypeForAceInhibitorDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val aceInhibitorNumeratorDf = MeasurementForAceInhibitorDf.select("member_sk")


    /*Final Numerator (union of all the sub numerator conditions)*/
    val cdc7Numerator = measurementForNephropathyScreeningDf.union(NephropathyTreatmentDf).union(measurementForCkdStage4Df).union(esrdDf).union(kidneyTranspalantDf).union(measurementForNephrologistDf).union(aceInhibitorNumeratorDf)
    val cdc7numeratorDf = cdc7Numerator.intersect(cdc7DinominatorForKpiCalDf)


    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.cdc7uptValueSet ::: KpiConstants.cdc7NtValueSet ::: KpiConstants.cdc3CkdStage4ValueSet ::: KpiConstants.cdc7EsrdValueSet ::: KpiConstants.cdc7KtValueSet
    val dinoExclReasonValueSet = KpiConstants.cdcDiabetesExclValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)

    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, cdc7numeratorDf, numExclDf, outReasonValueSet, data_source)
    //commonOutputFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factGapsInHedisTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.cdcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()


  }
}
