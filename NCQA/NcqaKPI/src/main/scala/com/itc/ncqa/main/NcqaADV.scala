package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConversions._

object NcqaADV {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAADV")
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
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.advMeasureTitle)

    /*Continous enrollment checking*/
    val contEnrollEndDate = year + "-12-31"
    val contEnrollStartDate = year + "-01-01"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*call the view based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {
      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {
      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the element who is present in the 45 or 60 days view*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), continuousEnrollDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")


    /*Age filter based on the measureId*/
    val ageFilterDf =  measureId match {

      case KpiConstants.adv1MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age2Val, KpiConstants.age3Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      case KpiConstants.adv2MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age4Val, KpiConstants.age6Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      case KpiConstants.adv3MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age7Val, KpiConstants.age10Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      case KpiConstants.adv4MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age11Val, KpiConstants.age14Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      case KpiConstants.adv5MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age15Val, KpiConstants.age18Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      case KpiConstants.adv6MeasureId => UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age19Val, KpiConstants.age20Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    }
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf)
    val dinominatorExclusionDf = hospiceDf
    /*ADV dinominator for kpi calculation*/
    val dinominatorAfterExclusionDf = dinominatorDf.except(dinominatorExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator (Dental Visits Value Set with dental practitioner during the measurement year)*/
    val dentalVisitValList = List(KpiConstants.dentalVisitsVal)
    val dentalVisitCodeVal = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForDentalVisitDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.advMeasureId, dentalVisitValList, dentalVisitCodeVal)

    /*Load the provider table to Memory*/
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)

    /*Join with DimProvider and filter based on dentist visit*/
    val joinedWithDimProviderDf = joinedForDentalVisitDf.as("df1").join(dimProviderDf.as("df2"), joinedForDentalVisitDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType)
                                                                         .filter($"df2.dentist".===(KpiConstants.yesVal)).select("df1.*")
    val measurForDentalVisitDf = UtilFunctions.measurementYearFilter(joinedWithDimProviderDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    val numeratorDf = measurForDentalVisitDf.intersect(dinominatorAfterExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*reason valueset */
    val numeratorValueSet = dentalVisitValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)

    /*create empty NumeratorExcldf*/
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
