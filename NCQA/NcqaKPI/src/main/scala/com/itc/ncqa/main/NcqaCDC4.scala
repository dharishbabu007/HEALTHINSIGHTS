package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import scala.collection.JavaConversions._

object NcqaCDC4 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC4")
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
    val cdc4DinominatorForKpiCalDf = dinoMemberSkDf.except(dinominatorExclusionDf)


    /*Numerator calculation*/

    /*Numerator First Condition*/
    val hedisJoinedForNumerator1Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForFirstNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val joinedWithDimProviderDf = hedisJoinedForNumerator1Df.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForNumerator1Df.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType).filter(dimProviderDf.col(KpiConstants.eyeCareProvColName).===(KpiConstants.yesVal))
    val measurementForFirstNumeratorDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator Second Condition*/
    val measurementForSecondNumerator = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementOneyearUpper, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator Third Condition*/
    val hedisJoinedForNumerator3Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForThirdNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val thirdNumeratorDf = measurementForSecondNumerator.as("df1").join(hedisJoinedForNumerator3Df.as("df2"), measurementForSecondNumerator.col(KpiConstants.memberskColName) === hedisJoinedForNumerator3Df.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk").distinct()


    /*Numerator Fourth Condition*/
    val hedisJoinedForEyeCareProfessionalDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForFourthNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val measurementForFourthNumeratorDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEyeCareProfessionalDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator Fifth Condition*/
    val measurementForFifthNumeratorDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEyeCareProfessionalDf, "start_date", year, KpiConstants.measurementOneyearUpper, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)


    /*Numerator Sixth Condition*/
    val hedisJoinedForDiabeticalretinalScreeningDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForSixthNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val measurementForSixthNumeratorDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDiabeticalretinalScreeningDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)


    /*Numerator Seventh Condition*/
    val hedisJoinedForUnilateralEyeUnicleation = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForUnilateralEyeUnicleation, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val hedisJoinedForBilateral = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForBilateral, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val joinSeventhNumerator = hedisJoinedForUnilateralEyeUnicleation.as("df1").join(hedisJoinedForBilateral.as("df2"), hedisJoinedForUnilateralEyeUnicleation.col(KpiConstants.memberskColName) === hedisJoinedForBilateral.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")


    /*Numerator Eight*/
    val hedisJoinedForUnilateralEyeEnuLeftDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4UnilateralEyeEnuLeftValueSet, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val selfJoin = hedisJoinedForUnilateralEyeEnuLeftDf.as("df1").join(hedisJoinedForUnilateralEyeEnuLeftDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "inner").select("df1.member_sk", "df1.start_date", "df2.start_date")
    //val selectedColumnDf = selfJoin.select("df1.member_sk","df1.start_date","df2.start_date").withColumn("second_date",$"df2.start_date")
    val daysDiffDf = selfJoin.filter(datediff(selfJoin.col("df2.start_date"), selfJoin.col("df1.start_date")).>=(14)).select("member_sk").distinct()


    /*Numerator Nine*/
    val hedisJoinedUnilateralEyeEnuRightDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4UnilateralEyeEnuRightValueSet, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val joinedData = hedisJoinedForUnilateralEyeEnuLeftDf.as("df1").join(hedisJoinedUnilateralEyeEnuRightDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).select("df1.member_sk").distinct()


    val unionOfAllNumeratorDf = measurementForFirstNumeratorDf.union(measurementForSecondNumerator).union(thirdNumeratorDf).union(measurementForFourthNumeratorDf).union(measurementForFifthNumeratorDf).union(measurementForSixthNumeratorDf).union(joinSeventhNumerator).union(daysDiffDf).union(joinedData).distinct()
    val cdc4NumeratorDf = unionOfAllNumeratorDf.intersect(cdc4DinominatorForKpiCalDf)


    /*common output format(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.cdc4ValueSetForFirstNumerator ::: KpiConstants.cdc4ValueSetForThirdNumerator ::: KpiConstants.cdc4ValueSetForFourthNumerator ::: KpiConstants.cdc4ValueSetForSixthNumerator ::: KpiConstants.cdc4ValueSetForUnilateralEyeUnicleation
    val dinoExclReasonValueSet = KpiConstants.cdcDiabetesExclValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cdc4MeasureId)

    /*formatted data creation*/
    val numExclDF = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, cdc4NumeratorDf, numExclDF, outReasonValueSet, sourceAndMsrList)
    commonOutputFormattedDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.cdcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()
  }
}
