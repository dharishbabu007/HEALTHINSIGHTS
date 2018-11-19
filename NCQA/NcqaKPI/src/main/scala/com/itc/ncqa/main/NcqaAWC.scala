package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConversions._

object NcqaAWC {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAAWC")
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


    /*Join dimmember,factclaim,factmembership,dimlocation,reflob and dimfacility table*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.awcMeasureTitle)



    /* val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk",KpiConstants.arrayOfColumn:_*)
     val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.*","df2.product_plan_sk").withColumnRenamed("start_date_sk","claim_start_date_sk")
     val ref_lobDf = spark.sql(KpiConstants.refLobLoadQuery)*/



    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {
      lookupTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {
      lookupTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*common filter checking*/
    val commonFilterDf = joinedForInitialFilterDf.as("df1").join(lookupTableDf.as("df2"), joinedForInitialFilterDf.col(KpiConstants.memberskColName) === lookupTableDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookupTableDf.col("start_date").isNull).select("df1.*")


    /*Dinominator for output format (Allowable age group)*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age12Val, KpiConstants.age21Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /*Dinominator For calculation*/
    val dinominatorDf = ageFilterDf.select(KpiConstants.memberskColName).distinct()

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Numerator1 (Well-Care Value Set as procedure code with PCP or an OB/GYN practitioner during the measurement year)*/
    val hedisJoinedForWcvAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.awcMeasureId, KpiConstants.awcWcvValueSet, KpiConstants.awcWcvCodeSystem)

    /*Loading the dimProvider table data*/
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)

    /*Join the hedisJoinedForWcvAsProDf with dimProviderDf for getting the elements who has obgyn as Y*/
    //val joinedWithDimProviderDf = hedisJoinedForWcvAsProDf.as("df1").join(dimProviderDf.as("df2"),$"df1.provider_sk" === $"df2.provider_sk","inner").filter($"df2.pcp".===("Y")  || ($"df2.obgyn".===("Y")))
    val joinedWithDimProviderDf = hedisJoinedForWcvAsProDf.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForWcvAsProDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType)
      .filter(dimProviderDf.col(KpiConstants.pcpColName).===(KpiConstants.yesVal) || dimProviderDf.col(KpiConstants.obgynColName).===(KpiConstants.yesVal))

    val measurementForWcvAsProDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("member_sk").distinct()


    /*Numerator2 (Well-Care Value Set as primary diagnosis with PCP or an OB/GYN practitioner during the measurement year)*/
    val hedisJoinedForWcvAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.awcMeasureId, KpiConstants.awcWcvValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    //val joinedWithDimProviderAsDiagDf = hedisJoinedForWcvAsDiagDf.as("df1").join(dimProviderDf.as("df2"),$"df1.provider_sk" === $"df2.provider_sk","inner").filter($"df2.pcp".===("Y")  || ($"df2.obgyn".===("Y")))

    val joinedWithDimProviderAsDiagDf = hedisJoinedForWcvAsDiagDf.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForWcvAsDiagDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType)
      .filter(dimProviderDf.col(KpiConstants.pcpColName).===(KpiConstants.yesVal) || dimProviderDf.col(KpiConstants.obgynColName).===(KpiConstants.yesVal))

    val measurementForWcvAsDiagDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderAsDiagDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("member_sk").distinct()
    val awcNumeratorUnionDf = measurementForWcvAsProDf.union(measurementForWcvAsDiagDf).distinct()
    val awcNumeratorDf = awcNumeratorUnionDf.intersect(dinominatorDf).distinct()

    //println("counts:"+dinominatorDf.count()+","+awcNumeratorDf.count())

    /*common output(data to fact_gaps_in_hedis)*/
    val numeratorValueSet = KpiConstants.awcWcvValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.awcMeasureId)

    /*create empty NumeratorExcldf*/
    val numExclDf = spark.emptyDataFrame
    val dinoExclDf = spark.emptyDataFrame

    /*Calling function for creating the output format*/
    val commonOutputFormatDf = UtilFunctions.commonOutputDfCreation(spark, ageFilterDf, dinoExclDf, awcNumeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    //commonOutputFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)


    /*Data loading to fact_hedis_qms table.*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.awcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()
  }
}
