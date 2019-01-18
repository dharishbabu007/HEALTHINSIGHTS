package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object NcqaW34 {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAW34")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
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

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.w34MeasureTitle)

    /*Continuous Enrollment Checking*/

    val contEnrollEndDate = year + "-12-31"
    val contEnrollStartDate = year + "-01-01"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))


    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) ) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), continuousEnrollDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age3Val, KpiConstants.age6Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()
    val dinominatorExclDf = hospiceDf
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    //dinominatorAfterExclusionDf.show()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator Calculation (Well-Care Value Set) as procedure code*/
    val w34NumeratorVal = List(KpiConstants.wellCareVal)
    val w34NumCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForWellCareDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.w34MeasureId, w34NumeratorVal, w34NumCodeSystem)
    val msrForWellCareDf = UtilFunctions.measurementYearFilter(joinedForWellCareDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName,KpiConstants.providerSkColName)

    /*Numerator Calculation (Well-Care Value Set) as primary diagnosis*/
    val joinedForWellCareAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.w34MeasureId , w34NumeratorVal, primaryDiagnosisCodeSystem)
    val msrForWellCareAsDiagDf = UtilFunctions.measurementYearFilter(joinedForWellCareAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName,KpiConstants.providerSkColName)

    /*Numerator Calculation (Union of  measurementForWellCareAsPrDf and measurementForWellCareAsDiagDf)*/
    val w34NumeratorGeneral = msrForWellCareDf.union(msrForWellCareAsDiagDf)

    /*Start_date_sk for members who have pcp equal to 'Y'*/

    val w34WellCareJoinDf = dimProviderDf.as("df1").join(w34NumeratorGeneral.as("df2"), ($"df1.$KpiConstants.providerSkColName" === $"df2.$KpiConstants.providerSkColName"), joinType = KpiConstants.innerJoinType)
      .filter(($"df1.$KpiConstants.pcpColName" === KpiConstants.yesVal))
      .select("df2.KpiConstants.memberskColName")

    /*Numerator*/
    val numeratorDf =  w34WellCareJoinDf.intersect(dinominatorAfterExclusionDf)
   // numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = w34NumeratorVal
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
