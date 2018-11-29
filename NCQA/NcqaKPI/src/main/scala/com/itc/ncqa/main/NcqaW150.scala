package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object NcqaW150 {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAW150")
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
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)


    /*Join dimMember,factclaim,factmembership,reflob,dimfacility,dimlocation.*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.w15MeasureTitle)


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

    /*doing age filter 15 months old during the measurement year */

    val expr = "year(date_add(dob,456)) = " + year
    val ageFilterDf = commonFilterDf.filter(expr)
    //ageFilterDf.orderBy("member_sk").show(50)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)


    /*Dinominator calculation */
    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    dinominatorForKpiCalDf.show()


    /*find out the hospice members*/

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()

    /*Dinominator After Exclusion eligible population*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    dinominatorAfterExclusionDf.show()

    /*Numerator Calculation*/

    /*Numerator Calculation (Well-Care Value Set) as procedure code*/
    val hedisJoinedForWellCareAsPrDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.w150MeasureId, KpiConstants.w15ValueSetForNumerator, KpiConstants.w15CodeSystemForNum)
    val measurementForWellCareAsPrDf = UtilFunctions.mesurementYearFilter(hedisJoinedForWellCareAsPrDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator Calculation (Well-Care Value Set) as primary diagnosis*/
    val hedisJoinedForWellCareAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.w150MeasureId, KpiConstants.w15ValueSetForNumerator, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForWellCareAsDiagDf = UtilFunctions.mesurementYearFilter(hedisJoinedForWellCareAsDiagDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator Calculation (Union of  measurementForWellCareAsPrDf and measurementForWellCareAsDiagDf)*/
    val w150NumeratorGeneral = measurementForWellCareAsPrDf.union(measurementForWellCareAsDiagDf)

    /*Start_date_sk for members who have pcp equal to 'Y'*/


    val w150WellCareJoinDf = dimProviderDf.as("df1").join(factClaimDf.as("df2"), ($"df1.provider_sk" === $"df2.provider_sk"), "inner").filter(($"df1.pcp" === KpiConstants.yesVal)).select("df2.member_sk", "df2.start_date_sk").distinct()

    val w150JoinNumerator = w150NumeratorGeneral.as("df1").join(w150WellCareJoinDf.as("df2"), ($"df1.member_sk" === $"df2.member_sk"), joinType = "inner").select("df2.member_sk", "df2.start_date_sk")

    val w150WellCareCountZeroDf = w150JoinNumerator.groupBy("member_sk").agg(count("start_date_sk").alias("count1")).filter($"count1".===(0)).select(KpiConstants.memberskColName)

    val w150NumeratorDf = w150WellCareCountZeroDf.intersect(dinominatorAfterExclusionDf)
    w150NumeratorDf.show()

    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.w15ValueSetForNumerator
    val dinoExclReasonValueSet = KpiConstants.emptyList
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source, KpiConstants.w150MeasureId)

    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorAfterExclusionDf, w150NumeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.cdcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()
  }
}