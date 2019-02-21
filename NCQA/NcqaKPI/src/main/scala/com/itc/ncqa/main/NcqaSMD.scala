package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util._

object NcqaSMD {
/*


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASMD")
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
    //    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.smdMeasureTitle)

    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.smdMeasureTitle)

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

    val smdBHStandValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val smdBHStandCodeSystem = List(KpiConstants.ubrevCodeVal)
    val hedisJoinedForBHStandDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdBHStandValueSet, smdBHStandCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForBHStandDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 -"Schizophrenia" as primary diagnosis common to all */

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val smdSchizophreniaICDValueSet = List(KpiConstants.schizophreniaVal)
    val hedisJoinedForSchizophreniaIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdSchizophreniaICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForSchizophreniaIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForSchizophreniaIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BHStandDf = measrForForBHStandDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 - "Visit Setting Unspecified","Acute Inpatient POS" as procedure code */

    val smdVisitSettingValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.acuteInpatientPosVal)
    val smdVisitSettingCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSettingValueSet, smdVisitSettingCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingIcdDf = UtilFunctions.mesurementYearFilter(measrForForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingDf = measrForVisitSettingIcdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* At least one acute inpatient encounter with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForOneAcuteInpatDf = step1BHStandDf.union(step1VisitSettingDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForOneAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinForOneAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val OneAcuteInpatDf = measrForOneAcuteInpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(1)).select(KpiConstants.memberskColName)

    /* Step1 2a- "Visit Setting Unspecified","Outpatient POS" procedure code  */

    val smdVisitSettingOutpatPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.outpatientPosVal)
    val smdVisitSettingOutpatPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingOutpatPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSettingOutpatPosValueSet, smdVisitSettingOutpatPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingOutpatPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSettingOutpatPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingOutpatPosDf = measrForVisitSettingOutpatPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSettingOutpatPosTwoVisitDf = step1VisitSettingOutpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2b- "BH Outpatient" procedure code  */

    val smdBhOutpatValueSet = List(KpiConstants.bhOutpatientVal)
    val smdBhOutpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdBhOutpatValueSet, smdBhOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhOutpatDf = measrForBhOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhOutpatTwoVisitDf = step1BhOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2c- "Visit Setting Unspecified","Partial Hospitalization POS" procedure code  */

    val smdVisitSetPartialValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.partialHospitalizationPosVal)
    val smdVisitSetPartialCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetPartialDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSetPartialValueSet, smdVisitSetPartialCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetPartialDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetPartialDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetPartialDf = measrForVisitSetPartialDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetPartialTwoVisitDf = step1VisitSetPartialDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2d- "Partial Hospitalization/Intensive Outpatient" procedure code  */

    val smdPartialHosOutpatValueSet = List(KpiConstants.partialHospitalizationIntensiveOutpatientVal)
    val smdPartialHosOutpatCodeSystem = List(KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForPartialHosOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdPartialHosOutpatValueSet, smdPartialHosOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForPartialHosOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPartialHosOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1PartialHosOutpatDf = measrForPartialHosOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1PartialHosOutpatTwoVisitDf = step1PartialHosOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2e- "Visit Setting Unspecified","Community Mental Health Center POS" procedure code  */

    val smdVisitSetCommValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.communityMentalHealthCenterPosVal)
    val smdVisitSetCommCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetCommDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSetCommValueSet, smdVisitSetCommCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetCommDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetCommDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetCommDf = measrForVisitSetCommDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetCommTwoVisitDf = step1VisitSetCommDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2f- "Electroconvulsive Therapy" procedure code  */

    val smdElectroTherapyValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val smdElectroTherapyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForElectroTherapyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdElectroTherapyValueSet, smdElectroTherapyCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /* Step1 2f- "Electroconvulsive Therapy" primary diagnosis  */

    val smdElectroTherapyICDValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val hedisJoinedForElectroTherapyIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdElectroTherapyICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1smdElectroTherapyICDDf = measrForElectroTherapyDf.union(measrForElectroTherapyIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ElectroTherapyDf = step1smdElectroTherapyICDDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1smdElectroTherapyICDTwoVisitDf = step1ElectroTherapyDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /* Step1 2g- "Observation" procedure code  */

    val smdObservValueSet = List(KpiConstants.observationVal)
    val smdObservCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForObservDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdObservValueSet, smdObservCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForObservDf = UtilFunctions.mesurementYearFilter(hedisJoinedForObservDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1ObservDf = measrForObservDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ObservTwoVisitDf = step1ObservDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /* Step1 2h- "ED" procedure code  */

    val smdEdValueSet = List(KpiConstants.edVal)
    val smdEdCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForEdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdEdValueSet, smdEdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForEdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1EdDf = measrForEdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1EdTwoVisitDf = step1EdDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2i "Visit Setting Unspecified","ED POS" procedure code  */

    val smdVisitSetEdPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.edPosVal)
    val smdVisitSetEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSetEdPosValueSet, smdVisitSetEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetEdPosDf = measrForVisitSetEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetEdPosTwoVisitDf = step1VisitSetEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2j- "BH Stand Alone Nonacute Inpatient","ED POS" procedure code  */

    val smdBhInpatEdPosValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal,KpiConstants.edPosVal)
    val smdBhInpatEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhInpatEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdBhInpatEdPosValueSet, smdBhInpatEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhInpatEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhInpatEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhInpatEdPosDf = measrForBhInpatEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhInpatEdPosTwoVisitDf = step1BhInpatEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2k- "Visit Setting Unspecified","Nonacute Inpatient POS" procedure code  */

    val smdVisitSetNonAcuteInpatValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val smdVisitSetNonAcuteInpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetNonAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSetNonAcuteInpatValueSet, smdVisitSetNonAcuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetNonAcuteInpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetNonAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetNonAcuteInpatPosDf = measrForVisitSetNonAcuteInpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetNonAcuteInpatPosTwoVisitDf = step1VisitSetNonAcuteInpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2l- "Visit Setting Unspecified","Telehealth POS" procedure code  */

    val smdVisitSetTelePosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val smdVisitSetTelePosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetTelePosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdVisitSetTelePosValueSet, smdVisitSetTelePosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetTelePosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetTelePosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetTelePosDf = measrForVisitSetTelePosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetTelePosTwoVisitDf = step1VisitSetTelePosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Join for At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting, on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForTwoVisitsOutpatDf = step1VisitSettingOutpatPosTwoVisitDf .union(step1BhOutpatTwoVisitDf).union(step1VisitSetPartialTwoVisitDf).union(step1PartialHosOutpatTwoVisitDf).union(step1VisitSetCommTwoVisitDf).union(step1smdElectroTherapyICDTwoVisitDf).union(step1ObservTwoVisitDf).union(step1EdTwoVisitDf).union(step1VisitSetEdPosTwoVisitDf).union(step1BhInpatEdPosTwoVisitDf).union(step1VisitSetNonAcuteInpatPosTwoVisitDf).union(step1VisitSetTelePosTwoVisitDf).select(KpiConstants.memberskColName)


    // val t = joinForTwoVisitsOutpatDf.limit(1).rdd.isEmpty()

    /*  At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting,  on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    //  val measrForTwoVisitsOutpatDf = UtilFunctions.mesurementYearFilter(joinForTwoVisitsOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /* Two of any of the following, with or without a telehealth modifier (Telehealth Modifier Value Set) */

    val smdTeleModifierValueSet = List(KpiConstants.telehealthModifierVal)
    val smdTeleModifierCodeSystem = List(KpiConstants.modifierCodeVal)
    val hedisJoinedForTeleModifierDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdTeleModifierValueSet, smdTeleModifierCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForTeleModifierDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleModifierDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /* Join  with telehealth modifier */

    val finalStep1SecondConditionDf = joinForTwoVisitsOutpatDf.union(measrForTeleModifierDf).select(KpiConstants.memberskColName)

    /* Identify members with schizophrenia or schizoaffective disorder as those who met at least one of the Step1 criteria during the measurement year */

    val finalStep1Df = OneAcuteInpatDf.union(finalStep1SecondConditionDf).select(KpiConstants.memberskColName)

    println("final Step1")

    finalStep1Df.show()

    /* Step1 logic End */

    /* Step2 begin */

    //<editor-fold desc="Event Diagnosis (Step2)">

    /*Event Diagnosis Step2 starts*/

    /*Accute Inpatient*/
    val joinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdaAcuteInpatientValueSet,KpiConstants.spdaAcuteInpatientCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)


    /*Diabetes Valueset*/
    val joinedForDiabetesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf,refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, KpiConstants.cdcDiabetesvalueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForDiab1Df = UtilFunctions.mesurementYearFilter(joinedForDiabetesDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)



    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"medication_list".isin(KpiConstants.spdDiabetesMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val diabetesValuesetDf = measurementForDiab1Df.union(MeasurementForSecondDinoDf)


    /*Telehealth Modifier valueset*/
    val joinedForTeleHealthDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdaTeleHealthValueSet,KpiConstants.spdaTeleHealthCodeSystem)
    val measurementForTeleHealthDf = UtilFunctions.mesurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Acute Inpatient Value Set with a Diabetes Value Set and without Telehealth Modifier Value Set; Telehealth POS Value Set*/
    val dinominator1Df = measurementAcuteInpatDf.intersect(diabetesValuesetDf).except(measurementForTeleHealthDf)


    /*at least 2 Outpatient visit*/
    val joinedForTwoOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdOutPatientValueSet,KpiConstants.spdOutPatientCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.mesurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val joinedForTwoObservationDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdObservationValueSet,KpiConstants.spdObservationCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.mesurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val joinedForTwoEdVisistsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdEdVisitValueSet,KpiConstants.spdEdVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.mesurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient*/
    val joinedForTwoNonAcutePatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdNonAcutePatValueSet,KpiConstants.spdNonAcutePatCodeSystem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.mesurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    val dinominator2UnionDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)
    /*atleast 2 visit with Outpatient  or Observation  or ED or Nonacute Inpatient  and has Diabetes */
    val dinominator2Df = dinominator2UnionDf.intersect(diabetesValuesetDf)
    //dinominator2Df.show()



    /*Telephone visit valueset*/
    val joinedForTelephoneVisitDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdTelephoneVisitValueSet,KpiConstants.spdTelephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.mesurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val telephoneVisitAndDiabetesDf =  mesrForTelephoneVisitDf.intersect(diabetesValuesetDf)

    /*Online Assesment valueset*/
    val joinedForOnlineAssesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdOnlineAssesValueSet,KpiConstants.spdOnlineAssesCodeSystem)
    val mesrForOnlineAssesDf = UtilFunctions.mesurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val onlineAssesAndDiabetesDf = mesrForOnlineAssesDf.intersect(diabetesValuesetDf)
    /*Dinominator(Step1 Union)*/
    val dinominatorUnionDf = dinominator1Df.union(dinominator2Df).union(telephoneVisitAndDiabetesDf).union(onlineAssesAndDiabetesDf)

    val finalStep2Df = dinominatorUnionDf
    /*Dinominator1(Step1) starts*/
    //</editor-fold>

    /* End of Step2 */

    /* Step1 output intersects step2 giving EligiblePopulation */

    val finalStep1InersectStep2Df = finalStep1Df.intersect(finalStep2Df).select(KpiConstants.memberskColName)

    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName).distinct()

    /* Exclusion Optional */ /* ----- Pending ---------*/
    /* Members do not have Diabetes Value Set and  Members who had Diabetes Exclusions Value Set */

    /* diabetes valueset measurementForDiab1Df */

    /*Diabetes Exclusion Valueset*/


    val dinominatorExcl = hospiceDf.select(KpiConstants.memberskColName)

    val denominatorWithContErollDf = finalStep1InersectStep2Df.union(continiousEnrollDf).select(KpiConstants.memberskColName)

    val finalDinomenatorDf = denominatorWithContErollDf.except(dinominatorExcl).select(KpiConstants.memberskColName)

    finalDinomenatorDf.show()

    /* Start Numerator */

    /* LDL-C Tests and HbA1c Tests Value Set */

    val smdLdlcHbA1cTestsValueSet = List(KpiConstants.ldlcTestsVal,KpiConstants.hba1cTestVal)
    val smdLdlcHbA1cTestsCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptCatIIVal,KpiConstants.loincCodeVal)
    val hedisJoinedForLdlcHbA1cTestsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.smdMeasureId, smdLdlcHbA1cTestsValueSet, smdLdlcHbA1cTestsCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForLdlcHbA1cTestsDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLdlcHbA1cTestsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)



    val finalNumeratorDf = measrForLdlcHbA1cTestsDf.intersect(finalDinomenatorDf)

    finalNumeratorDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/

    val numeratorValueSet = smdLdlcHbA1cTestsValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.smdMeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, denominatorWithContErollDf, dinominatorExcl, finalNumeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)

    spark.sparkContext.stop()

  }

*/

}

