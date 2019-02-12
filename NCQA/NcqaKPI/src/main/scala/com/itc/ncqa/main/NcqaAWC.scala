package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{add_months, to_date}

import scala.collection.JavaConversions._
import scala.collection.mutable

object NcqaAWC {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

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

    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val factMemAttrDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName, KpiConstants.factMemAttrTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val dimQualityMsrDf = DataLoadFunctions.dimqualityMeasureLoadFunction(spark,KpiConstants.awcMeasureTitle)
    val dimQualityPgmDf = DataLoadFunctions.dimqualityProgramLoadFunction(spark, KpiConstants.hedisPgmname)
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

    //<editor-fold desc="Age filter">

    val ageFilterDf = UtilFunctions.ageFilter(initialJoinedDf, KpiConstants.dobColName, year, KpiConstants.age12Val, KpiConstants.age22Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberskColName, KpiConstants.benefitMedicalColname, KpiConstants.memStartDateColName,KpiConstants.memEndDateColName)
    val argMap = mutable.Map(KpiConstants.dateStartKeyName -> contEnrollStartDate, KpiConstants.dateEndKeyName -> contEnrollEndDate, KpiConstants.dateAnchorKeyName -> contEnrollEndDate,
                             KpiConstants.lobNameKeyName -> lob_name, KpiConstants.benefitKeyName -> KpiConstants.benefitMedicalColname)
    val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                  .select("df1.*")
    //</editor-fold>


    /*Eligible Population Exclusion (Hospice)*/
    val requiredExclDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)

    /*eligible population( After the exclusion of Hospice members)*/
    val elgiblePopDf = contEnrollDf.select("*").as("df1").join(requiredExclDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                   .filter($"df2.${KpiConstants.memberskColName}".isNull)
                                   .select("df1.*")

    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val denominatorDf = elgiblePopDf.select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val dfMapForNumCalculation = mutable.Map(KpiConstants.eligibleDfName -> denominatorDf, KpiConstants.factClaimTblName -> factClaimDf , KpiConstants.refHedisTblName -> refHedisDf)
    //<editor-fold desc="Numerator1 (Well-Care Value Set as procedure code with PCP or an OB/GYN practitioner during the measurement year)">

    val wellcareValList = List(KpiConstants.wellCareVal)
    val wellcareCodeVal = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val joinedForWcvAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForNumCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.awcMeasureId, wellcareValList, wellcareCodeVal)
                                           .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))

    val measurForWcvAsProDf = UtilFunctions.measurementYearFilter(joinedForWcvAsProDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                           .select(KpiConstants.memberskColName,KpiConstants.providerSkColName)

    /*Join the hedisJoinedForWcvAsProDf with dimProviderDf for getting the elements who has obgyn as Y*/
    val wcvAsProceedureDf = measurForWcvAsProDf.as("df1").join(dimProviderDf.as("df2"), $"df1.${KpiConstants.providerSkColName}" === $"df2.${KpiConstants.providerSkColName}", KpiConstants.innerJoinType)
                                                                      .filter(dimProviderDf.col(KpiConstants.pcpColName).===(KpiConstants.yesVal) || dimProviderDf.col(KpiConstants.obgynColName).===(KpiConstants.yesVal))
                                                                      .select(s"df1.${KpiConstants.memberskColName}")

    //</editor-fold>

    //<editor-fold desc="Numerator2 (Well-Care Value Set as primary diagnosis with PCP or an OB/GYN practitioner during the measurement year)">

    val primaryDiagCodeVal = List(KpiConstants.icdCodeVal)
    val joinedForWcvAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForNumCalculation, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.awcMeasureId, wellcareValList, primaryDiagCodeVal)
                                            .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))

    val measurForWcvAsDiagDf = UtilFunctions.measurementYearFilter(joinedForWcvAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                            .select(KpiConstants.memberskColName, KpiConstants.providerSkColName)

    val wcvAsPrimDiagDf = measurForWcvAsDiagDf.as("df1").join(dimProviderDf.as("df2"), $"df1.${KpiConstants.providerSkColName}" === $"df2.${KpiConstants.providerSkColName}", KpiConstants.innerJoinType)
                                                               .filter(dimProviderDf.col(KpiConstants.pcpColName).===(KpiConstants.yesVal) || dimProviderDf.col(KpiConstants.obgynColName).===(KpiConstants.yesVal))
                                                               .select(s"df1.${KpiConstants.memberskColName}")

    //</editor-fold>

    val numeratorDf = wcvAsProceedureDf.union(wcvAsPrimDiagDf)

    //</editor-fold>

    /*
    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = wellcareValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)

    /*create empty NumeratorExcldf*/
    val numExclDf = spark.emptyDataFrame
    val dinominatorExclDf = spark.emptyDataFrame

    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>
*/
    spark.sparkContext.stop()
  }
}
