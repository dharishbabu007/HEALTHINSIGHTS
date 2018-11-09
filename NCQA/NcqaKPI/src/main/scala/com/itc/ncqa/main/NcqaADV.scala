package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConversions._

object NcqaADV {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAADV")
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


    /*Join dimMember,Factclaim,FactMembership,RefLocation,DimFacilty for initail filter*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.advMeasureTitle)

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

    /*Dinominator for output format (age between 2 and 20)*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age2Val, KpiConstants.age20Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    val dinominatorDf = ageFilterDf.select(KpiConstants.memberskColName).distinct()
    /*Dinominator Exclusion*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf)
    val measurementDinominatorExclDf = UtilFunctions.mesurementYearFilter(hospiceDf, "start_date", year, 0, 365).select("member_sk").distinct()

    /*ADV dinominator for kpi calculation*/
    val advDinominatorForKpiCal = dinominatorDf.except(measurementDinominatorExclDf)

    /*Numerator (Dental Visits Value Set with dental practitioner during the measurement year)*/
    val hedisJoinedForDentalVisitDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.advMeasureId, KpiConstants.advOutpatientValueSet, KpiConstants.advOutpatientCodeSystem)

    /*Load the provider table to Memory*/
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)

    /*Join with DimProvider and filter based on dentist visit*/
    val joinedWithDimProviderDf = hedisJoinedForDentalVisitDf.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForDentalVisitDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType).filter($"df2.dentist".===(KpiConstants.yesVal))
    val measurementForDentalVisitDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("member_sk").distinct()
    val advNumeratorDf = measurementForDentalVisitDf.intersect(advDinominatorForKpiCal)
    print("counts:" + dinominatorDf.count() + "," + measurementDinominatorExclDf.count() + "," + advNumeratorDf.count())
    /*Numerator Ends*/


    /*Common output creation starts(data to fact_gaps_in_hedis table)*/

    /*reason valueset */
    val numeratorValueSet = KpiConstants.advOutpatientValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)


    /*create empty NumeratorExcldf*/
    val numExclDf = spark.emptyDataFrame
    val outFormattedDf = UtilFunctions.commonOutputDfCreation(spark, ageFilterDf, measurementDinominatorExclDf, advNumeratorDf, numExclDf, outValueSetForOutput, data_source)
    //outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factGapsInHedisTblName)


    /*Data loading to fact_hedis_qms table.*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.advMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()
  }
}
