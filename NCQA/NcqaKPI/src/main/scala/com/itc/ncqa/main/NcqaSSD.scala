package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util._

object NcqaSSD {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and Spark session object creation">

    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source = ""
    val measureId = args(4)

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAOMW")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required tables to Memory">

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Continuous Enrollment, Allowable Gap and Age filter">

    /*Initial join function*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.ssdMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Allowable Gap filter */
    val lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*Age filter*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)

    //<editor-fold desc="Step1">

    //<editor-fold desc="Step1Sub1">

    /*"BH Stand Alone Acute Inpatient" as procedure code */
    val ssdBHStandValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val ssdBHStandCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinedForBHStandDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBHStandValueSet, ssdBHStandCodeSystem)
    val measrForForBHStandDf = UtilFunctions.measurementYearFilter(joinedForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                            .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*"Schizophrenia" , "Bipolar Disorder", "Other Bipolar Disorder" as primary diagnosis common */
    val ssdSchizophreniaICDValueSet = List(KpiConstants.schizophreniaVal,KpiConstants.bipolarDisorderVal,KpiConstants.otherBipolarDisorder)
    val joinedForSchizophreniaIcdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdSchizophreniaICDValueSet, primaryDiagCodeSystem)
    val measrForSchizophreniaIcdDf = UtilFunctions.measurementYearFilter(joinedForSchizophreniaIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                  .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    /*"BH Stand Alone Acute Inpatient" with "Schizophrenia" or "Bipolar Disorder" or "Other Bipolar Disorder"*/
    val bhAndschbipdobipdDf = measrForForBHStandDf.select(KpiConstants.memberskColName).intersect(measrForSchizophreniaIcdDf.select(KpiConstants.memberskColName))


    /*"Visit Setting Unspecified" valueset */
    val ssdVisitSettingValueSet = List(KpiConstants.visitSettingUnspecifiedVal)
    val ssdVisitSettingCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val joinedForVisitSettingDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSettingValueSet, ssdVisitSettingCodeSystem)
    val measrForVisitSettingIcdDf = UtilFunctions.measurementYearFilter(joinedForVisitSettingDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                 .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*"Acute Inpatient POS" valueset*/
    val acuteInPatPosValList = List(KpiConstants.acuteInpatientPosVal)
    val acuteInPatPosCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.posCodeVal)
    val joinedForacuteInpatposDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, acuteInPatPosValList, acuteInPatPosCodeSystem)
    val messrForacuteInpatposDf = UtilFunctions.measurementYearFilter(joinedForVisitSettingDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                               .select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /* At least one acute inpatient encounter with any diagnosis of schizophrenia or schizoaffective disorder */
    val visitUnspecAndscbpdobpdDf = measrForVisitSettingIcdDf.select(KpiConstants.memberskColName).intersect(messrForacuteInpatposDf.select(KpiConstants.memberskColName)).intersect(measrForSchizophreniaIcdDf.select(KpiConstants.memberskColName))

    /*At least one acute inpatient encounter, with any diagnosis of schizophrenia, schizoaffective disorder or bipolar disorder*/
    val step1Sub1Df = bhAndschbipdobipdDf.union(visitUnspecAndscbpdobpdDf)
    //</editor-fold>

    //<editor-fold desc="Step1Sub2">

    /*Telehealth Modifier*/
    val telehealthModValList = List(KpiConstants.telehealthModifierVal)
    val telehealthModCodeSystem = List(KpiConstants.posCodeVal)
    val joinedForTelehealthDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, telehealthModValList, telehealthModCodeSystem)
    val measrForTelehealthDf = UtilFunctions.measurementYearFilter(joinedForTelehealthDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                            .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    //<editor-fold desc="Visit Setting Unspecified with Outpatient POS">

    /* "Visit Setting Unspecified" */
    val ssdVisitSetUnspecValueSet = List(KpiConstants.visitSettingUnspecifiedVal)
    val ssdVisitSetUnspecCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForssdVisitSetUnspecDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetUnspecValueSet, ssdVisitSetUnspecCodeSystem)
    val measrForVisitSetUnspecDf = UtilFunctions.measurementYearFilter(joinedForssdVisitSetUnspecDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                 .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* "Outpatient POS" */
    val outPatPosValList = List(KpiConstants.outpatientPosVal)
    val outPatPosCodeSystem = List(KpiConstants.posCodeVal)
    val joinedForoutPatPosDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, outPatPosValList, outPatPosCodeSystem)
    val measrForoutPatPosDf = UtilFunctions.measurementYearFilter(joinedForoutPatPosDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                           .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*"Visit Setting Unspecified" and "Outpatient POS"*/
    val visSetOutpatPosDf = measrForVisitSetUnspecDf.intersect(measrForoutPatPosDf)
    val disVisSetOutpatPosDf = visSetOutpatPosDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="BH Outpatient">

    /* Step1 2b- "BH Outpatient" procedure code  */
    val ssdBhOutpatValueSet = List(KpiConstants.bhOutpatientVal)
    val ssdBhOutpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForBhOutpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBhOutpatValueSet, ssdBhOutpatCodeSystem)
    val measrForBhOutpatDf = UtilFunctions.measurementYearFilter(joinedForBhOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                          .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val disBhOutpatDf = measrForBhOutpatDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Visit Setting Unspecified with Partial Hospitalization POS">

    /* Step1 2c- "Visit Setting Unspecified","Partial Hospitalization POS" procedure code  */
    val ssdPartialValueSet = List(KpiConstants.partialHospitalizationPosVal)
    val ssdPartialCodeSystem = List(KpiConstants.posCodeVal)
    val joinedForPartialDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdPartialValueSet, ssdPartialCodeSystem)
    val measrForPartialDf = UtilFunctions.measurementYearFilter(joinedForPartialDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                 .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val vissetunAndPartialDf = measrForVisitSetUnspecDf.intersect(measrForPartialDf)
    val disvissetunAndPartialDf = vissetunAndPartialDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Partial Hospitalization/Intensive Outpatient">

    /* Step1 2d- "Partial Hospitalization/Intensive Outpatient" procedure code  */
    val ssdPartialHosOutpatValueSet = List(KpiConstants.partialHospitalizationIntensiveOutpatientVal)
    val ssdPartialHosOutpatCodeSystem = List(KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForPartialHosOutpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdPartialHosOutpatValueSet, ssdPartialHosOutpatCodeSystem)
    val measrForPartialHosOutpatDf = UtilFunctions.measurementYearFilter(joinedForPartialHosOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                  .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val disPartialHosOutpatDf = measrForPartialHosOutpatDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Visit Setting Unspecified with Community Mental Health Center POS">

    /* Step1 2e- "Visit Setting Unspecified","Community Mental Health Center POS" procedure code  */
    val ssdCommentalValueSet = List(KpiConstants.communityMentalHealthCenterPosVal)
    val ssdCommentalCodeSystem = List(KpiConstants.posCodeVal)
    val joinedForCommentalDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdCommentalValueSet, ssdCommentalCodeSystem)
    val measrForCommentalDf = UtilFunctions.measurementYearFilter(joinedForCommentalDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                           .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val visitSetCommentalDf = measrForVisitSetUnspecDf.intersect(measrForCommentalDf)
    val disvisitSetCommentalDf = visitSetCommentalDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Electroconvulsive Therapy">

    /* Step1 2f- "Electroconvulsive Therapy" procedure code  */
    val ssdElectroTherapyValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val ssdElectroTherapyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForElectroTherapyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdElectroTherapyValueSet, ssdElectroTherapyCodeSystem)
    val measrForElectroTherapyDf = UtilFunctions.measurementYearFilter(joinedForElectroTherapyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                .select(KpiConstants.memberskColName,KpiConstants.startDateColName)



    /* Step1 2f- "Electroconvulsive Therapy" primary diagnosis  */
    val ssdElectroTherapyICDValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val joinedForElectroTherapyDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdElectroTherapyICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyDiagDf = UtilFunctions.measurementYearFilter(joinedForElectroTherapyDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                                    .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val electroTherapyDf = measrForElectroTherapyDf.union(measrForElectroTherapyDiagDf)
    val diselectroTherapyDf = electroTherapyDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))

    //</editor-fold>

    //<editor-fold desc="Observation">

    /* Step1 2g- "Observation" procedure code  */
    val ssdObservValueSet = List(KpiConstants.observationVal)
    val ssdObservCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForObservDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdObservValueSet, ssdObservCodeSystem)
    val measrForObservDf = UtilFunctions.measurementYearFilter(joinedForObservDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                        .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val disObservDf = measrForObservDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="ED">

    /* Step1 2h- "ED" procedure code  */
    val ssdEdValueSet = List(KpiConstants.edVal)
    val ssdEdCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForEdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdEdValueSet, ssdEdCodeSystem)
    val measrForEdDf = UtilFunctions.measurementYearFilter(joinedForEdDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                    .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val disEdDf = measrForEdDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Visit Setting Unspecified with ED POS">

    /* Step1 2i "Visit Setting Unspecified","ED POS" procedure code */
    val ssdVisitSetEdPosValueSet = List(KpiConstants.edPosVal)
    val ssdVisitSetEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val joinedForEdPosDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetEdPosValueSet, ssdVisitSetEdPosCodeSystem)
    val measrForEdPosDf = UtilFunctions.measurementYearFilter(joinedForEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                       .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val visitSetEdPosDf = measrForVisitSetUnspecDf.intersect(measrForEdPosDf)

    val disvisitSetEdPosDf = visitSetEdPosDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="BH Stand Alone Nonacute Inpatient">

    /* Step1 2j- "BH Stand Alone Nonacute Inpatient" procedure code  */
    val ssdBhInpatEdPosValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val ssdBhInpatEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForBhInpatEdPosDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBhInpatEdPosValueSet, ssdBhInpatEdPosCodeSystem)
    val measrForBhInpatEdPosDf = UtilFunctions.measurementYearFilter(joinedForBhInpatEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                              .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val disBhInpatEdPosDf = measrForBhInpatEdPosDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Visit Setting Unspecified with Nonacute Inpatient POS">

    /* Step1 2k- "Visit Setting Unspecified","Nonacute Inpatient POS" procedure code  */
    val ssdVisitSetNonAcuteInpatValueSet = List(KpiConstants.nonacuteInpatientPosVal)
    val ssdVisitSetNonAcuteInpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val joinedForNonAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetNonAcuteInpatValueSet, ssdVisitSetNonAcuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForNonAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForNonAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                               .select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val visitSetNonAcuteInpatDf = measrForVisitSetUnspecDf.intersect(measrForNonAcuteInpatDf)

    val disvisitSetNonAcuteInpatDf = visitSetNonAcuteInpatDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    //<editor-fold desc="Visit Setting Unspecified with Telehealth POS">

    /* Step1 2l- "Visit Setting Unspecified","Telehealth POS" procedure code  */
    val ssdVisitSetTelePosValueSet = List(KpiConstants.telephonePosVal)
    val ssdVisitSetTelePosCodeSystem = List(KpiConstants.posCodeVal)
    val joinedForTelePosDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetTelePosValueSet, ssdVisitSetTelePosCodeSystem)
    val measrForTelePosDf = UtilFunctions.measurementYearFilter(joinedForTelePosDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                         .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val visitSetTelePosDf = measrForVisitSetUnspecDf.intersect(measrForTelePosDf)
    val disvisitSetTelePosDf = visitSetTelePosDf.groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.startDateColName}").alias(KpiConstants.startDateColName))
    //</editor-fold>

    /*Union of all sub Dfs with member_sk  and max date if more than one start_date present for one member_sk */
    val unionOfAllDistDf = disVisSetOutpatPosDf.union(disBhOutpatDf).union(disvissetunAndPartialDf).union(disPartialHosOutpatDf)
                                               .union(disvisitSetCommentalDf).union(diselectroTherapyDf).union(disObservDf)
                                               .union(disEdDf).union(disvisitSetEdPosDf).union(disBhInpatEdPosDf)
                                               .union(disvisitSetNonAcuteInpatDf).union(disvisitSetTelePosDf)

    val atleast2visitDf = unionOfAllDistDf.as("df1").join(unionOfAllDistDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                           .filter(abs(datediff($"df1.${KpiConstants.startDateColName}", $"df2.${KpiConstants.startDateColName}")).>(0))
                                                           .select(s"df1.${KpiConstants.memberskColName}")
    val step1Sub2Df = (atleast2visitDf.intersect(measrForTelehealthDf.select(KpiConstants.memberskColName))).intersect(measrForSchizophreniaIcdDf.select(KpiConstants.memberskColName))
    //</editor-fold>

    val step1Df = step1Sub1Df.union(step1Sub2Df)
    //</editor-fold>

    //<editor-fold desc="Step2">

    //<editor-fold desc="Step2Sub1">

    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)


    /*Diabetes Valueset*/
    val diabtesValList = List(KpiConstants.diabetesVal)
    val joinedForDiabetesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, diabtesValList, primaryDiagCodeSystem)
    val measurementForDiab1Df = UtilFunctions.measurementYearFilter(joinedForDiabetesDf, "start_date", year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val diabetesValuesetDf = measurementForDiab1Df


    /*Telehealth Modifier and Telehealth Modifier valueset*/
    val teleHealAndModValList = List(KpiConstants.telehealthModifierVal, KpiConstants.telehealthPosVal)
    val teleHealAndModCodeSsytem = List(KpiConstants.modifierCodeVal, KpiConstants.posCodeVal)
    val joinedForTeleHealthDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,teleHealAndModValList,teleHealAndModCodeSsytem)
    val measurementForTeleHealthDf = UtilFunctions.measurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Acute Inpatient Value Set with a Diabetes Value Set and without Telehealth Modifier Value Set; Telehealth POS Value Set*/
    val dinominator1Sub1Df = measurementAcuteInpatDf.intersect(diabetesValuesetDf).except(measurementForTeleHealthDf)



    /*at least 2 Outpatient visit*/
    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,outPatValList,outPatCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*Telephone visit valueset*/
    val telephoneVisitValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTelephoneVisitDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,telephoneVisitValList,telephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.measurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val telephoneVisitAndDiabetesDf =  mesrForTelephoneVisitDf.intersect(diabetesValuesetDf)

    /*Online Assesment valueset*/
    val onlineVisitValList = List(KpiConstants.onlineAssesmentVal)
    val onlineVisitCodeSytsem = List(KpiConstants.cptCodeVal)
    val joinedForOnlineAssesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,onlineVisitValList,onlineVisitCodeSytsem)
    val mesrForOnlineAssesDf = UtilFunctions.measurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val onlineAssesAndDiabetesDf = mesrForOnlineAssesDf.intersect(diabetesValuesetDf)

    val twoOutPatAndTeleDf = twoOutPatDf.intersect(telephoneVisitAndDiabetesDf)
    /*at least 2 Outpatient visit,Telephone visit valueset, Online Assesment valueset*/
    val outPatTeleOnlineDf = twoOutPatAndTeleDf.intersect(onlineAssesAndDiabetesDf)


    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient without Telehealth*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.ssdMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    val acuteInPatwoTeleDf = twoNonAcutePatDf.except(measurementForTeleHealthDf)


    val dinominator2UnionDf = outPatTeleOnlineDf.union(twoObservationDf).union(twoEdVisitDf).union(acuteInPatwoTeleDf)
    /*atleast 2 visit with Outpatient  or Observation  or ED or Nonacute Inpatient  and has Diabetes */
    val dinominator1Sub2Df = dinominator2UnionDf.intersect(diabetesValuesetDf)



    /*Diabetic Medication checking*/
    val diabeticMedicationValList = List(KpiConstants.diabetesMedicationVal)
    val joinedForDiabeticsmedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,diabeticMedicationValList)
    val dinominator1Sub3Df = UtilFunctions.measurementYearFilter(joinedForDiabeticsmedDf,KpiConstants.rxStartDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)


    /*Dinominator(Step2Sub1 Union)*/
    val step2Sub1Df = dinominator1Sub1Df.union(dinominator1Sub2Df).union(dinominator1Sub3Df)
    /*Dinominator1(Step1) ends*/
    //</editor-fold>

    //<editor-fold desc="Step2Sub2">

    /*Long-Acting Injections valueset during measurement year*/
    val longActingInjValList = List(KpiConstants.longActingInjVal)
    val longActingInjCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForlongActingInjDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, longActingInjValList, longActingInjCodeSystem)
    val measrForlongActingInjDf = UtilFunctions.measurementYearFilter(joinedForlongActingInjDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                               .select(KpiConstants.memberskColName)



    val antipsychoticMedValList = List(KpiConstants.antipsychoticMedVal)
    val joinedForantipsyMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark, factRxClaimsDf,ref_medvaluesetDf,KpiConstants.ssdMeasureId, antipsychoticMedValList)
    val mesrForantipsyMedDf = UtilFunctions.measurementYearFilter(joinedForantipsyMedDf,KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                           .select(KpiConstants.memberskColName)

    /*antipsychoticMedication */
    val step2Sub2Df = measrForlongActingInjDf.union(mesrForantipsyMedDf)
    //</editor-fold>

    val step2Df = step2Sub1Df.union(step1Sub2Df)
    //</editor-fold>


    /* Step1 output except step2 giving EligiblePopulation */
    val dinominatorUnionDf = step1Df.except(step2Df)
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                    .select("df1.*")
    val dinominatorForKpiDf = dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName).distinct()
    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName)
    val dinominatorAfterExclDf = dinominatorForKpiDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /* LDL-C Tests and HbA1c Tests Value Set */
    val ssdglucoseTestsHbA1cTestsValueSet = List(KpiConstants.glucoseTestsValueSet,KpiConstants.hba1cTestVal)
    val ssdglucoseTestsHbA1cTestsCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptcat2CodeVal,KpiConstants.loincCodeVal)
    val joinedForglucoseHbA1cDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdglucoseTestsHbA1cTestsValueSet, ssdglucoseTestsHbA1cTestsCodeSystem)
    val measrForglucoseHbA1cDf = UtilFunctions.measurementYearFilter(joinedForglucoseHbA1cDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                              .select(KpiConstants.memberskColName)

    val numeratorDf = measrForglucoseHbA1cDf.intersect(dinominatorAfterExclDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = ssdglucoseTestsHbA1cTestsValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()

  }

}

