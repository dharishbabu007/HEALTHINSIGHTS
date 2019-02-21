package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util._

object NcqaSMC {

  /*

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASMC")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
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
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)

    /*Initial join function call for prepare the data fro common filter*/
    //    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.smcMeasureTitle)

    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.smcMeasureTitle)

    initialJoinedDf.show()

    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")



    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)



    //   val continiousEnrollDf = ageFilterDf

    /*Continous enrollment calculation*/
    val contEnrollStrtDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continiousEnrollDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(ageFilterDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate))).select(KpiConstants.memberskColName)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)


    /* Step1 - "BH Stand Alone Acute Inpatient" as procedure code */

    val smcBHStandValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val smcBHStandCodeSystem = List(KpiConstants.ubrevCodeVal)
    val hedisJoinedForBHStandDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcBHStandValueSet, smcBHStandCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForBHStandDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 -"Schizophrenia" as primary diagnosis common to all */

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val smcSchizophreniaICDValueSet = List(KpiConstants.schizophreniaVal)
    val hedisJoinedForSchizophreniaIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcSchizophreniaICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForSchizophreniaIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForSchizophreniaIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BHStandDf = measrForForBHStandDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 - "Visit Setting Unspecified","Acute Inpatient POS" as procedure code */

    val smcVisitSettingValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.acuteInpatientPosVal)
    val smcVisitSettingCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSettingValueSet, smcVisitSettingCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingIcdDf = UtilFunctions.mesurementYearFilter(measrForForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingDf = measrForVisitSettingIcdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* At least one acute inpatient encounter with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForOneAcuteInpatDf = step1BHStandDf.union(step1VisitSettingDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForOneAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinForOneAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val OneAcuteInpatDf = measrForOneAcuteInpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(1)).select(KpiConstants.memberskColName)

    /* Step1 2a- "Visit Setting Unspecified","Outpatient POS" procedure code  */

    val smcVisitSettingOutpatPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.outpatientPosVal)
    val smcVisitSettingOutpatPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingOutpatPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSettingOutpatPosValueSet, smcVisitSettingOutpatPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingOutpatPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSettingOutpatPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingOutpatPosDf = measrForVisitSettingOutpatPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSettingOutpatPosTwoVisitDf = step1VisitSettingOutpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2b- "BH Outpatient" procedure code  */

    val smcBhOutpatValueSet = List(KpiConstants.bhOutpatientVal)
    val smcBhOutpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcBhOutpatValueSet, smcBhOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhOutpatDf = measrForBhOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhOutpatTwoVisitDf = step1BhOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2c- "Visit Setting Unspecified","Partial Hospitalization POS" procedure code  */

    val smcVisitSetPartialValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.partialHospitalizationPosVal)
    val smcVisitSetPartialCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetPartialDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSetPartialValueSet, smcVisitSetPartialCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetPartialDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetPartialDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetPartialDf = measrForVisitSetPartialDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetPartialTwoVisitDf = step1VisitSetPartialDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2d- "Partial Hospitalization/Intensive Outpatient" procedure code  */

    val smcPartialHosOutpatValueSet = List(KpiConstants.partialHospitalizationIntensiveOutpatientVal)
    val smcPartialHosOutpatCodeSystem = List(KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForPartialHosOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcPartialHosOutpatValueSet, smcPartialHosOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForPartialHosOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPartialHosOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1PartialHosOutpatDf = measrForPartialHosOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1PartialHosOutpatTwoVisitDf = step1PartialHosOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2e- "Visit Setting Unspecified","Community Mental Health Center POS" procedure code  */

    val smcVisitSetCommValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.communityMentalHealthCenterPosVal)
    val smcVisitSetCommCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetCommDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSetCommValueSet, smcVisitSetCommCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetCommDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetCommDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetCommDf = measrForVisitSetCommDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetCommTwoVisitDf = step1VisitSetCommDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2f- "Electroconvulsive Therapy" procedure code  */

    val smcElectroTherapyValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val smcElectroTherapyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForElectroTherapyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcElectroTherapyValueSet, smcElectroTherapyCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /* Step1 2f- "Electroconvulsive Therapy" primary diagnosis  */

    val smcElectroTherapyICDValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val hedisJoinedForElectroTherapyIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcElectroTherapyICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1smcElectroTherapyICDDf = measrForElectroTherapyDf.union(measrForElectroTherapyIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ElectroTherapyDf = step1smcElectroTherapyICDDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1smcElectroTherapyICDTwoVisitDf = step1ElectroTherapyDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2g- "Observation" procedure code  */

    val smcObservValueSet = List(KpiConstants.observationVal)
    val smcObservCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForObservDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcObservValueSet, smcObservCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForObservDf = UtilFunctions.mesurementYearFilter(hedisJoinedForObservDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1ObservDf = measrForObservDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ObservTwoVisitDf = step1ObservDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /* Step1 2h- "ED" procedure code  */

    val smcEdValueSet = List(KpiConstants.edVal)
    val smcEdCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForEdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcEdValueSet, smcEdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForEdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1EdDf = measrForEdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1EdTwoVisitDf = step1EdDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2i "Visit Setting Unspecified","ED POS" procedure code  */

    val smcVisitSetEdPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.edPosVal)
    val smcVisitSetEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSetEdPosValueSet, smcVisitSetEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetEdPosDf = measrForVisitSetEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetEdPosTwoVisitDf = step1VisitSetEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2j- "BH Stand Alone Nonacute Inpatient","ED POS" procedure code  */

    val smcBhInpatEdPosValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal,KpiConstants.edPosVal)
    val smcBhInpatEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhInpatEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcBhInpatEdPosValueSet, smcBhInpatEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhInpatEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhInpatEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhInpatEdPosDf = measrForBhInpatEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhInpatEdPosTwoVisitDf = step1BhInpatEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2k- "Visit Setting Unspecified","Nonacute Inpatient POS" procedure code  */

    val smcVisitSetNonAcuteInpatValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val smcVisitSetNonAcuteInpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetNonAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSetNonAcuteInpatValueSet, smcVisitSetNonAcuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetNonAcuteInpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetNonAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetNonAcuteInpatPosDf = measrForVisitSetNonAcuteInpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetNonAcuteInpatPosTwoVisitDf = step1VisitSetNonAcuteInpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2l- "Visit Setting Unspecified","Telehealth POS" procedure code  */

    val smcVisitSetTelePosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val smcVisitSetTelePosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetTelePosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcVisitSetTelePosValueSet, smcVisitSetTelePosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetTelePosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetTelePosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetTelePosDf = measrForVisitSetTelePosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetTelePosTwoVisitDf = step1VisitSetTelePosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Join for At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting, on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForTwoVisitsOutpatDf = step1VisitSettingOutpatPosTwoVisitDf .union(step1BhOutpatTwoVisitDf).union(step1VisitSetPartialTwoVisitDf).union(step1PartialHosOutpatTwoVisitDf).union(step1VisitSetCommTwoVisitDf).union(step1smcElectroTherapyICDTwoVisitDf).union(step1ObservTwoVisitDf).union(step1EdTwoVisitDf).union(step1VisitSetEdPosTwoVisitDf).union(step1BhInpatEdPosTwoVisitDf).union(step1VisitSetNonAcuteInpatPosTwoVisitDf).union(step1VisitSetTelePosTwoVisitDf).select(KpiConstants.memberskColName)


    // val t = joinForTwoVisitsOutpatDf.limit(1).rdd.isEmpty()

    /*  At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting,  on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    //  val measrForTwoVisitsOutpatDf = UtilFunctions.mesurementYearFilter(joinForTwoVisitsOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /* Two of any of the following, with or without a telehealth modifier (Telehealth Modifier Value Set) */

    val smcTeleModifierValueSet = List(KpiConstants.telehealthModifierVal)
    val smcTeleModifierCodeSystem = List(KpiConstants.modifierCodeVal)
    val hedisJoinedForTeleModifierDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcTeleModifierValueSet, smcTeleModifierCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForTeleModifierDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleModifierDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /* Join  with telehealth modifier */

    val finalStep1SecondConditionDf = joinForTwoVisitsOutpatDf.union(measrForTeleModifierDf).select(KpiConstants.memberskColName)

    /* Identify members with schizophrenia or schizoaffective disorder as those who met at least one of the Step1 criteria during the measurement year */

    val finalStep1Df = OneAcuteInpatDf.union(finalStep1SecondConditionDf).select(KpiConstants.memberskColName)

    println("final Step1")

    finalStep1Df.show()

    /* Step1 logic End */

    /* Step2 begin */
    /* Step2 Event Identify members of step1 who also had cardio vascular disease */
    /* Step2 "AMI" primary diagnosis  */

    val smcAmiICDValueSet = List(KpiConstants.amiVal)
    val hedisJoinedForAmiICDDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcAmiICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForAmiICDDf = UtilFunctions.mesurementYearFilter(hedisJoinedForAmiICDDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /* Identify all acute and nonacute inpatient stays (Inpatient Stay Value Set) */

    val smcnonacuteInpatValueSet = List(KpiConstants.inPatientStayVal)
    val smcnonacuteInpatCodeSystem = List(KpiConstants.ubrevCodeVal)
    val hedisJoinedFornonacuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcnonacuteInpatValueSet, smcnonacuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrFornonacuteInpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleModifierDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)


    /* AMI. Discharged from an inpatient setting  */

    val finalamiWithInpatDf = measrForAmiICDDf.intersect(measrFornonacuteInpatDf)

    /* CABG. Members who had CABG (CABG Value Set) in any setting  as primary diagnosis*/

    val smcCabgICDValueSet = List(KpiConstants.cabgVal)
    val hedisJoinedForCabgICDDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcCabgICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForCabgICDDf = UtilFunctions.mesurementYearFilter(hedisJoinedForCabgICDDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /* CABG. Members who had CABG (CABG Value Set) in any setting  as procedure code */

    val smcCabgValueSet = List(KpiConstants.cabgVal)
    val smcCabgCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForCabgDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcCabgValueSet, smcCabgCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForCabgDf = UtilFunctions.mesurementYearFilter(hedisJoinedForCabgDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val finalCabgDf = measrForCabgICDDf.union(measrForCabgDf)

    /*	PCI. Members who had PCI (PCI Value Set) in any setting (e.g., inpatient, outpatient, ED) as primary diagnosis */

    val smcPciICDValueSet = List(KpiConstants.pctVal)
    val hedisJoinedForPciICDDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcPciICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForPciICDDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPciICDDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /* PCI. Members who had PCI (PCI Value Set) in any setting (e.g., inpatient, outpatient, ED) as procedure code */

    val smcPciValueSet = List(KpiConstants.pctVal)
    val smcPciCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForPciDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcPciValueSet, smcPciCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForPciDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPciDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val finalPciDf = measrForPciICDDf.union(measrForPciDf).select(KpiConstants.memberskColName)

    val finalEventDf = finalamiWithInpatDf.union(finalCabgDf).union(finalPciDf).select(KpiConstants.memberskColName)

    /* End Step2 Event */

    /* Start step2 Diagnosis. Identify members with IVD */

    /*	IVD as primary diagnosis */

    val smcIvdICDValueSet = List(KpiConstants.ivdVal)
    val hedisJoinedForIvdICDDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcIvdICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForIvdICDDf = UtilFunctions.mesurementYearFilter(hedisJoinedForIvdICDDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    //    val measrForIvdICDWithStartDateDf = UtilFunctions.mesurementYearFilter(hedisJoinedForIvdICDDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    /* An Outpatient Value Set) as procedure code */

    val smcOutpatValueSet = List(KpiConstants.outPatientVal)
    val smcOutpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcOutpatValueSet, smcOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* An Outpatient Value Set) with any diagnosis of IVD (IVD Value Set) */

    val finalOutpatDf = measrForOutpatDf.intersect(measrForIvdICDDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* An Telephone Visits Value Set as procedure code */

    val smcTeleVisitValueSet = List(KpiConstants.telephoneVisitsVal)
    val smcTeleVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForTeleVisitDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcTeleVisitValueSet, smcTeleVisitCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForTeleVisitDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* An Telephone Visits Value Set with any diagnosis of IVD (IVD Value Set) */

    val finalTeleVisitDf = measrForTeleVisitDf.intersect(measrForIvdICDDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /* An online assessment (Online Assessments Value Set) as procedure code */

    val smcOnlineAssessValueSet = List(KpiConstants.pctVal)
    val smcOnlineAssessCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForOnlineAssessDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcOnlineAssessValueSet, smcOnlineAssessCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForOnlineAssessDf = UtilFunctions.mesurementYearFilter(hedisJoinedForOnlineAssessDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* An online assessment (Online Assessments Value Set) with any diagnosis of IVD (IVD Value Set) */

    val finalOnlineAssessDf = measrForOnlineAssessDf.intersect(measrForIvdICDDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* acute inpatient encounter (Acute Inpatient Value Set) as procedure code */

    val smcAcuteInpatValueSet = List(KpiConstants.accuteInpatVal)
    val smcAcuteInpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcAcuteInpatValueSet, smcAcuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForAcuteInpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* (Acute Inpatient Value Set) with a diagnosis of IVD (IVD Value Set) */

    val finalAcuteInpatDf = measrForAcuteInpatDf.intersect(measrForIvdICDDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* telehealth (Telehealth Modifier Value Set; Telehealth POS Value Set) */

    val smcTeleModifierPosValueSet = List(KpiConstants.telehealthModifierVal,KpiConstants.telehealthPosVal)
    val smcTeleModifierPosCodeSystem = List(KpiConstants.modifierCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForTeleModifierPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcTeleModifierPosValueSet, smcTeleModifierPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForTeleModifierPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleModifierPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*without telehealth (Telehealth Modifier Value Set; Telehealth POS Value Set)*/

    val finalAcuteInpatExceptTeleModifierPosDf = finalAcuteInpatDf.except(measrForTeleModifierPosDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* At least one acute inpatient encounter */

    val step2AcuteInpatOneVisitDf = finalAcuteInpatExceptTeleModifierPosDf.groupBy(KpiConstants.memberskColName,KpiConstants.startDateColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(1)).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val finalDiagnosisDf = finalOutpatDf.union(finalTeleVisitDf).union(finalOnlineAssessDf).union(step2AcuteInpatOneVisitDf).select(KpiConstants.memberskColName)

    /* End of step2 Diagnosis */

    /* final Step2 Data union event and diagnosis */

    val finalStep2Df = finalEventDf.union(finalDiagnosisDf).select(KpiConstants.memberskColName)

    /* End of Step2 */

    /* Step1 output intersects step2 giving EligiblePopulation */

    val finalStep1InersectStep2Df = finalStep1Df.intersect(finalStep2Df).select(KpiConstants.memberskColName)

    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName).distinct()

    val dinominatorExcl = hospiceDf.select(KpiConstants.memberskColName)

    val denominatorWithContErollDf = finalStep1InersectStep2Df.union(continiousEnrollDf).select(KpiConstants.memberskColName)

    val finalDinomenatorDf = denominatorWithContErollDf.except(dinominatorExcl).select(KpiConstants.memberskColName)

    finalDinomenatorDf.show()

    /* Start Numerator */

    /* LDL-C Tests Value Set */

    val smcLdlcTestsValueSet = List(KpiConstants.ldlcTestsVal)
    val smcLdlcTestsCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptCatIIVal,KpiConstants.loincCodeVal)
    val hedisJoinedForLdlcTestsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smcMeasureId, smcLdlcTestsValueSet, smcLdlcTestsCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForLdlcTestsDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLdlcTestsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val finalNumeratorDf = measrForLdlcTestsDf.intersect(finalDinomenatorDf)

    finalNumeratorDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/

    val numeratorValueSet = smcLdlcTestsValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.smcMeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, denominatorWithContErollDf, dinominatorExcl, finalNumeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)

    spark.sparkContext.stop()

  }


*/

}

