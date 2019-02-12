package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConversions._
import scala.collection.mutable

object NcqaABA {


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

    /* /*creating spark session object*/
     val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIMA")
     conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
     val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()*/

    import spark.implicits._

    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val factMemAttrDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName, KpiConstants.factMemAttrTblName,data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    /*val dimQualityMsrDf = DataLoadFunctions.dimqualityMeasureLoadFunction(spark,KpiConstants.abaMeasureTitle)
    val dimQualityPgmDf = DataLoadFunctions.dimqualityProgramLoadFunction(spark, KpiConstants.hedisPgmname)*/
    val dimProductPlanDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName,KpiConstants.dimProductTblName,data_source)
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Eligible Population Calculation">

    /*Initial join function call for prepare the data fro common filter*/
    val argmapForInitJoin = mutable.Map(KpiConstants.dimMemberTblName -> dimMemberDf, KpiConstants.factMembershipTblName -> factMembershipDf,
                                        KpiConstants.dimProductTblName -> dimProductPlanDf, KpiConstants.refLobTblName -> refLobDf,
                                        KpiConstants.factMemAttrTblName -> factMemAttrDf, KpiConstants.dimDateTblName -> dimDateDf)
    val initialJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForInitJoin)
    //<editor-fold desc="Age Filter">

    val age_filter_upperDate = year + "-12-31"
    val age_filter_lowerDate = year.toInt -1 + "-01-01"
    val ageFilterDf = initialJoinedDf.filter((add_months($"${KpiConstants.dobColName}", KpiConstants.months216).<=(age_filter_lowerDate)) && (add_months($"${KpiConstants.dobColName}", KpiConstants.months888).>=(age_filter_upperDate)))
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap, Benefit checking">

    val contEnrollStartDate = year.toInt -1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberskColName, KpiConstants.benefitMedicalColname, KpiConstants.memStartDateColName,KpiConstants.memEndDateColName)
    val argMap = mutable.Map(KpiConstants.dateStartKeyName -> contEnrollStartDate, KpiConstants.dateEndKeyName -> contEnrollEndDate, KpiConstants.dateAnchorKeyName -> contEnrollEndDate,
                             KpiConstants.lobNameKeyName -> lob_name, KpiConstants.benefitKeyName -> KpiConstants.benefitMedicalColname)
    val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                   .select("df1.*")
    //</editor-fold>

    val dfMapForCalculation = mutable.Map(KpiConstants.eligibleDfName -> contEnrollDf, KpiConstants.factClaimTblName -> factClaimDf , KpiConstants.refHedisTblName -> refHedisDf)

    //<editor-fold desc="Outpatient Event">

    val abaOutPatientValSet = List(KpiConstants.outPatientVal)
    val abaOutPatientCodeSystem = List(KpiConstants.hcpsCodeVal,KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForCalculation,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaOutPatientValSet,abaOutPatientCodeSystem)
    val measurForOutPatDf = UtilFunctions.measurementYearFilter(hedisJoinedForOutPatDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
    //</editor-fold>

    /*Eligible Population Exclusion (Hospice)*/
    val requiredExclDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)

    /*eligible population( After the exclusion of Hospice members)*/
    val elgiblePopDf = measurForOutPatDf.select("*").as("df1").join(requiredExclDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                        .filter($"df2.${KpiConstants.memberskColName}".isNull)
                                        .select("df1.*")

    //</editor-fold>

    //<editor-fold desc="Denominator Calculation">

    val denominatorDf = elgiblePopDf
    //dinominator.select(KpiConstants.memberskColName).show()
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val eligMembersForOptExclDf = denominatorDf.filter($"${KpiConstants.genderColName}".===(KpiConstants.femaleVal))
                                               .select(KpiConstants.memberskColName)
    val dfMapForOptExclCalculation = mutable.Map(KpiConstants.eligibleDfName -> eligMembersForOptExclDf, KpiConstants.factClaimTblName -> factClaimDf , KpiConstants.refHedisTblName -> refHedisDf)
    /*(Pregnancy Value Set during year or previous year)*/
    val abaPregnancyValSet = List(KpiConstants.pregnancyVal)
    val abaPregnancyCodeSystem = List(KpiConstants.icdCodeVal)
    val joinForPregnancyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForOptExclCalculation,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaPregnancyValSet,abaPregnancyCodeSystem)
    val measForPregnancyDf = UtilFunctions.measurementYearFilter(joinForPregnancyDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)

    val optionalExclDf = measForPregnancyDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val dfMapForNumCalculation = mutable.Map(KpiConstants.eligibleDfName -> denominatorDf, KpiConstants.factClaimTblName -> factClaimDf , KpiConstants.refHedisTblName -> refHedisDf)

    //<editor-fold desc="Numerator1 Calculation(BMI Value Set for 20 years or older)">

    val abaBmiValSet = List(KpiConstants.bmiVal)
    val abaBmiCodeSystem = List(KpiConstants.icdCodeVal)
    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val joinedForBmiValDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForNumCalculation,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaBmiValSet,abaBmiCodeSystem)
    val bmiNumeratorDf = UtilFunctions.measurementYearFilter(joinedForBmiValDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val)
                                    .filter(($"${KpiConstants.claimstatusColName}".isin(claimStatusList)) && (add_months($"${KpiConstants.dobColName}", KpiConstants.months240).<=($"${KpiConstants.serviceDateColName}")))
                                    .select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Numerator2 Calculation(BMI Percentile Value Set for age between 18 and 20)">

    /*Numerator2 Calculation*/
    val abaBmiPercentileValSet = List(KpiConstants.bmiPercentileVal)
    val abaBmiPercentileCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForBmiPercValDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForNumCalculation,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaBmiPercentileValSet,abaBmiPercentileCodeSystem)
    val bmiPercNumeratorDf = UtilFunctions.measurementYearFilter(joinedForBmiPercValDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                              .filter(($"${KpiConstants.claimstatusColName}".isin(claimStatusList)) && (add_months($"${KpiConstants.dobColName}", KpiConstants.months240).>($"${KpiConstants.serviceDateColName}")))
                                              .select(KpiConstants.memberskColName)

    //</editor-fold>

    /* ABA Numerator */
    val abaNumeratorDf = bmiNumeratorDf.union(bmiPercNumeratorDf)


    //</editor-fold>


/*
    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = abaBmiValSet ::: abaBmiPercentileValSet
    val dinominatorExclValueSet = abaPregnancyValSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorForOutput, dinominatorExcl, abanumeratorFinalDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>
*/
    spark.sparkContext.stop()
  }
}
