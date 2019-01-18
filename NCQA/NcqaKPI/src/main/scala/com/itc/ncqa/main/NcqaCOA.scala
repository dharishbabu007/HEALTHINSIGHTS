package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.directory.shared.kerberos.exceptions.ErrorType
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, dayofyear, expr, lit, to_date, when}

object NcqaCOA {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACOA")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
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

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.coaMeasureTitle)

    /*Allowable gap filtering*/
    val lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*doing age filter 66 years or more the measurement year */

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age66Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /*Continous enrollment checking*/
    val contEnrollEndDate = year + "-12-31"
    val contEnrollStartDate = year + "-01-01"
    val contEnrollDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && ageFilterDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = contEnrollDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()
    val dinominatorExclDf = hospiceDf
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    //dinominatorAfterExclusionDf.show()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="COA1">

    /*COA1 (Advance Care Planning Value Set during the measurement year) as procedure code*/
    val coa1Val = List(KpiConstants.advanceCarePlanning)
    val coa1CodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptCatIIVal,KpiConstants.hcpsCodeVal)
    val joinForAdvanceCareDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa1Val, coa1CodeSystem)
    val measrForAdvanceCareDf = UtilFunctions.measurementYearFilter(joinForAdvanceCareDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                             .select(KpiConstants.memberskColName)

    /*Numerator1 (Advance Care Planning Value Set during the measurement year) as primary diagnosis*/
    val joinForAdvanceCareIcdDf =  UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa1Val, primaryDiagnosisCodeSystem)
    val measrForAdvanceCareIcdDf = UtilFunctions.measurementYearFilter(joinForAdvanceCareIcdDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                                .select(KpiConstants.memberskColName)

    val coa1NumeratorDf = measrForAdvanceCareDf.union(measrForAdvanceCareIcdDf)

    //</editor-fold>

    //<editor-fold desc="COA2">
    /*COA2 a.(Medication review during the measurement year) as procedure code*/

    val coa2MedReviewVal = List(KpiConstants.medicationReviewVal)
    val coa2MedReviewCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptCatIIVal)
    val joinForMediReviewDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType,  KpiConstants.coaMeasureId, coa2MedReviewVal, coa2MedReviewCodeSystem)
    val measrCoa2aDf = UtilFunctions.measurementYearFilter(joinForMediReviewDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)

    /*COA2 b.(Medication List Value Set during the measurement year) as procedure code*/

    val coa2MedListVal = List(KpiConstants.medicationListVal)
    val coa2MedListCodeSystem = List(KpiConstants.hcpsCodeVal,KpiConstants.cptCatIIVal)
    val joinForMedListDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa2MedListVal, coa2MedListCodeSystem)
    val measrCoa2bDf = UtilFunctions.measurementYearFilter(joinForMedListDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    /*	Both of the following during the same visit */

    val coa2a2bSameDateDf = measrCoa2aDf.as("df1").join(measrCoa2bDf.as("df2"), (($"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}") && ($"df1.${KpiConstants.providerSkColName}" === $"df2.${KpiConstants.providerSkColName}"))
                                        , KpiConstants.innerJoinType)
                                        .filter(datediff($"df1.${KpiConstants.startDateColName}", $"df2.${KpiConstants.startDateColName}").===(KpiConstants.days0))
                                        .select(s"df1.${KpiConstants.memberskColName}",s"df1.${KpiConstants.providerSkColName}")


    /* provider type is a prescribing practitioner or clinical pharmacist */

    val coa2a2bSameProviderDf = dimProviderDf.as("df1").join(coa2a2bSameDateDf.as("df2"), ($"df1.$KpiConstants.providerSkColName" === $"df2.$KpiConstants.providerSkColName"), joinType = KpiConstants.innerJoinType)
      .filter(($"df1.${KpiConstants.provprespriColName}" === KpiConstants.yesVal) || ($"df1.${KpiConstants.clinphaColName}" === KpiConstants.yesVal))
      .select(s"df2.${KpiConstants.memberskColName}")

    /*	Transitional care management services  */

    val coa2MedSub2Val = List(KpiConstants.transitionalCareMgtSerVal)
    val coa2MedSub2CodeSystem = List(KpiConstants.cptCodeVal)
    val joinForMedSub2Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa2MedListVal, coa2MedListCodeSystem)
    val measrCoa2MedSub2Df = UtilFunctions.measurementYearFilter(joinForMedSub2Df , KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                          .select(KpiConstants.memberskColName)

    val coa2Df = coa2a2bSameProviderDf.union(measrCoa2MedSub2Df)

    /* Numerator Exclude services provided in an (Acute Inpatient Value Set; Acute Inpatient POS Value Set) */
    val coa2MedExclVal = List(KpiConstants.acuteInpatientVal,KpiConstants.acuteInpatientPosVal)
    val coa2MedExclCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal)
    val joinForMedExclDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa2MedExclVal, coa2MedExclCodeSystem)
    val measrCoaMedExclDf = UtilFunctions.measurementYearFilter(joinForMedExclDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                         .select(KpiConstants.memberskColName)

    val coa2NumeratorDf = coa2Df.except(measrCoaMedExclDf)
    //</editor-fold>

    //<editor-fold desc="COA3">
    /* COA3 Functional Status Assessment */

    val coa3Val = List(KpiConstants.functionalStatusVal)
    val coa3CodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptCatIIVal,KpiConstants.hcpsCodeVal)
    val joinCoa3Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa3Val, coa3CodeSystem)
    val measrCoa3Df = UtilFunctions.measurementYearFilter(joinCoa3Df, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                   .select(KpiConstants.memberskColName)

    val coa3NumeratorDf = measrCoa3Df.except(measrCoaMedExclDf)
    //</editor-fold>

    //<editor-fold desc="COA4">

    val coa4Val = List(KpiConstants.painAssessmentVal)
    val coa4CodeSystem = List(KpiConstants.cptCatIIVal)
    val joinCoa4Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.coaMeasureId, coa4Val, coa4CodeSystem)
    val measrCoa4Df = UtilFunctions.measurementYearFilter(joinCoa4Df, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                   .select(KpiConstants.memberskColName)

    val coa4NumeratorDf = measrCoa4Df.except(measrCoaMedExclDf)
    //</editor-fold>

    var numeratorDf = spark.emptyDataFrame
    var numeratorVal = List.empty[String]

    measureId match {

      case KpiConstants.coa1MeasureId =>  numeratorDf = coa1NumeratorDf.intersect(dinominatorAfterExclusionDf)
                                          numeratorVal = coa1Val

      case KpiConstants.coa2MeasureId =>  numeratorDf = coa2NumeratorDf.intersect(dinominatorAfterExclusionDf)
                                          numeratorVal = coa2MedReviewVal:::coa2MedListVal:::coa2MedSub2Val

      case KpiConstants.coa3MeasureId =>  numeratorDf = coa3NumeratorDf.intersect(dinominatorAfterExclusionDf)
                                          numeratorVal = coa3Val

      case KpiConstants.coa4MeasureId =>  numeratorDf = coa4NumeratorDf.intersect(dinominatorAfterExclusionDf)
                                          numeratorVal = coa4Val
    }

    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/

    val numeratorValueSet = numeratorVal
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