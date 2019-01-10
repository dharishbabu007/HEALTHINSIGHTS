package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util._

object NcqaSSD {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASSD")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
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
    //    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.ssdMeasureTitle)

    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.ssdMeasureTitle)

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

    val ssdBHStandValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val ssdBHStandCodeSystem = List(KpiConstants.ubrevCodeVal)
    val hedisJoinedForBHStandDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBHStandValueSet, ssdBHStandCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForBHStandDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 -"Schizophrenia" as primary diagnosis common to all */

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val ssdSchizophreniaICDValueSet = List(KpiConstants.schizophreniaVal,KpiConstants.bipolarDisorderVal,KpiConstants.otherBipolarDisorder)
    val hedisJoinedForSchizophreniaIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdSchizophreniaICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForForSchizophreniaIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForSchizophreniaIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BHStandDf = measrForForBHStandDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* Step1 - "Visit Setting Unspecified","Acute Inpatient POS" as procedure code */

    val ssdVisitSettingValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.acuteInpatientPosVal)
    val ssdVisitSettingCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSettingValueSet, ssdVisitSettingCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingIcdDf = UtilFunctions.mesurementYearFilter(measrForForBHStandDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingDf = measrForVisitSettingIcdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /* At least one acute inpatient encounter with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForOneAcuteInpatDf = step1BHStandDf.union(step1VisitSettingDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForOneAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinForOneAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val OneAcuteInpatDf = measrForOneAcuteInpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(1)).select(KpiConstants.memberskColName)

    /* Step1 2a- "Visit Setting Unspecified","Outpatient POS" procedure code  */

    val ssdVisitSettingOutpatPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.outpatientPosVal)
    val ssdVisitSettingOutpatPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSettingOutpatPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSettingOutpatPosValueSet, ssdVisitSettingOutpatPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSettingOutpatPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSettingOutpatPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSettingOutpatPosDf = measrForVisitSettingOutpatPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSettingOutpatPosTwoVisitDf = step1VisitSettingOutpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2b- "BH Outpatient" procedure code  */

    val ssdBhOutpatValueSet = List(KpiConstants.bhOutpatientVal)
    val ssdBhOutpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBhOutpatValueSet, ssdBhOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhOutpatDf = measrForBhOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhOutpatTwoVisitDf = step1BhOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2c- "Visit Setting Unspecified","Partial Hospitalization POS" procedure code  */

    val ssdVisitSetPartialValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.partialHospitalizationPosVal)
    val ssdVisitSetPartialCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetPartialDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetPartialValueSet, ssdVisitSetPartialCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetPartialDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetPartialDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetPartialDf = measrForVisitSetPartialDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetPartialTwoVisitDf = step1VisitSetPartialDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2d- "Partial Hospitalization/Intensive Outpatient" procedure code  */

    val ssdPartialHosOutpatValueSet = List(KpiConstants.partialHospitalizationIntensiveOutpatientVal)
    val ssdPartialHosOutpatCodeSystem = List(KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForPartialHosOutpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdPartialHosOutpatValueSet, ssdPartialHosOutpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForPartialHosOutpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPartialHosOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1PartialHosOutpatDf = measrForPartialHosOutpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1PartialHosOutpatTwoVisitDf = step1PartialHosOutpatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2e- "Visit Setting Unspecified","Community Mental Health Center POS" procedure code  */

    val ssdVisitSetCommValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.communityMentalHealthCenterPosVal)
    val ssdVisitSetCommCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetCommDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetCommValueSet, ssdVisitSetCommCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetCommDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetCommDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetCommDf = measrForVisitSetCommDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetCommTwoVisitDf = step1VisitSetCommDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2f- "Electroconvulsive Therapy" procedure code  */

    val ssdElectroTherapyValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val ssdElectroTherapyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForElectroTherapyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdElectroTherapyValueSet, ssdElectroTherapyCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /* Step1 2f- "Electroconvulsive Therapy" primary diagnosis  */

    val ssdElectroTherapyICDValueSet = List(KpiConstants.electroconvulsiveTherapyVal)
    val hedisJoinedForElectroTherapyIcdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdElectroTherapyICDValueSet, primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForElectroTherapyIcdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForElectroTherapyIcdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1ssdElectroTherapyICDDf = measrForElectroTherapyDf.union(measrForElectroTherapyIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ElectroTherapyDf = step1ssdElectroTherapyICDDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ssdElectroTherapyICDTwoVisitDf = step1ElectroTherapyDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2g- "Observation" procedure code  */

    val ssdObservValueSet = List(KpiConstants.observationVal)
    val ssdObservCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForObservDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdObservValueSet, ssdObservCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForObservDf = UtilFunctions.mesurementYearFilter(hedisJoinedForObservDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1ObservDf = measrForObservDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1ObservTwoVisitDf = step1ObservDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /* Step1 2h- "ED" procedure code  */

    val ssdEdValueSet = List(KpiConstants.edVal)
    val ssdEdCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForEdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdEdValueSet, ssdEdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForEdDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1EdDf = measrForEdDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1EdTwoVisitDf = step1EdDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2i "Visit Setting Unspecified","ED POS" procedure code  */

    val ssdVisitSetEdPosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.edPosVal)
    val ssdVisitSetEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetEdPosValueSet, ssdVisitSetEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetEdPosDf = measrForVisitSetEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetEdPosTwoVisitDf = step1VisitSetEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2j- "BH Stand Alone Nonacute Inpatient" procedure code  */

    val ssdBhInpatEdPosValueSet = List(KpiConstants.bHStandAloneAcuteInpatientVal)
    val ssdBhInpatEdPosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForBhInpatEdPosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdBhInpatEdPosValueSet, ssdBhInpatEdPosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForBhInpatEdPosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBhInpatEdPosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1BhInpatEdPosDf = measrForBhInpatEdPosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1BhInpatEdPosTwoVisitDf = step1BhInpatEdPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2k- "Visit Setting Unspecified","Nonacute Inpatient POS" procedure code  */

    val ssdVisitSetNonAcuteInpatValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val ssdVisitSetNonAcuteInpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetNonAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetNonAcuteInpatValueSet, ssdVisitSetNonAcuteInpatCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetNonAcuteInpatDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetNonAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetNonAcuteInpatPosDf = measrForVisitSetNonAcuteInpatDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetNonAcuteInpatPosTwoVisitDf = step1VisitSetNonAcuteInpatPosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Step1 2l- "Visit Setting Unspecified","Telehealth POS" procedure code  */

    val ssdVisitSetTelePosValueSet = List(KpiConstants.visitSettingUnspecifiedVal,KpiConstants.nonacuteInpatientPosVal)
    val ssdVisitSetTelePosCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.posCodeVal)
    val hedisJoinedForVisitSetTelePosDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdVisitSetTelePosValueSet, ssdVisitSetTelePosCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForVisitSetTelePosDf = UtilFunctions.mesurementYearFilter(hedisJoinedForVisitSetTelePosDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val step1VisitSetTelePosDf = measrForVisitSetTelePosDf.union(measrForForSchizophreniaIcdDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val step1VisitSetTelePosTwoVisitDf = step1VisitSetTelePosDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /* Join for At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting, on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    val joinForTwoVisitsOutpatDf = step1VisitSettingOutpatPosTwoVisitDf .union(step1BhOutpatTwoVisitDf).union(step1VisitSetPartialTwoVisitDf).union(step1PartialHosOutpatTwoVisitDf).union(step1VisitSetCommTwoVisitDf).union(step1ssdElectroTherapyICDTwoVisitDf).union(step1ObservTwoVisitDf).union(step1EdTwoVisitDf).union(step1VisitSetEdPosTwoVisitDf).union(step1BhInpatEdPosTwoVisitDf).union(step1VisitSetNonAcuteInpatPosTwoVisitDf).union(step1VisitSetTelePosTwoVisitDf).select(KpiConstants.memberskColName)


    // val t = joinForTwoVisitsOutpatDf.limit(1).rdd.isEmpty()

    /*  At least two visits in an outpatient, intensive outpatient, partial hospitalization, ED or nonacute inpatient setting,  on different dates of service, with any diagnosis of schizophrenia or schizoaffective disorder */

    //  val measrForTwoVisitsOutpatDf = UtilFunctions.mesurementYearFilter(joinForTwoVisitsOutpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /* Two of any of the following, with or without a telehealth modifier (Telehealth Modifier Value Set) */

    /*  val ssdTeleModifierValueSet = List(KpiConstants.telehealthModifierVal)
      val ssdTeleModifierCodeSystem = List(KpiConstants.modifierCodeVal)
      val hedisJoinedForTeleModifierDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdTeleModifierValueSet, ssdTeleModifierCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
      val measrForTeleModifierDf = UtilFunctions.mesurementYearFilter(hedisJoinedForTeleModifierDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    */ /* ----not part of the requirement ----------- */
    /* Join  with telehealth modifier */

    val finalStep1SecondConditionDf = joinForTwoVisitsOutpatDf.select(KpiConstants.memberskColName)

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

    /* An antipsychotic medication (Long-Acting Injections Value Set) */

    val longActingInjectionsCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForLongActingInjectionsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.ssdLongActingInjectionsValueSet,longActingInjectionsCodeSystem)
    val mesrForLongActingInjectionsDf = UtilFunctions.mesurementYearFilter(joinedForLongActingInjectionsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /* SSD Antipsychotic Medications List */

    val medValuesetForAntipsychotic = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter(($"medication_list".isin(KpiConstants.ssdAntipsychoticMedicationListVal:_*))&& ($"measure_id" === KpiConstants.ssdMeasureId)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForAntipsychotic = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForAntipsychotic = startDateValAddedDfForAntipsychotic.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForAntipsychoticDf = UtilFunctions.mesurementYearFilter(dateTypeDfForAntipsychotic, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /* Union Long-Acting Injections Value Set and  SSD Antipsychotic Medications List */

    val longLastingUnionAntipsychoticDf = mesrForLongActingInjectionsDf.union(MeasurementForAntipsychoticDf)

    val finalStep2Df = dinominatorUnionDf.union(longLastingUnionAntipsychoticDf)
    /*Dinominator1(Step1) starts*/
    //</editor-fold>

    /* End of Step2 */

    /* Step1 output except step2 giving EligiblePopulation */

    val finalStep1InersectStep2Df = finalStep1Df.except(finalStep2Df).select(KpiConstants.memberskColName)

    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName,KpiConstants.startDateColName).distinct()


    val dinominatorExcl = hospiceDf.select(KpiConstants.memberskColName)

    val denominatorWithContErollDf = finalStep1InersectStep2Df.union(continiousEnrollDf).select(KpiConstants.memberskColName)

    val finalDinomenatorDf = denominatorWithContErollDf.except(dinominatorExcl).select(KpiConstants.memberskColName)

    finalDinomenatorDf.show()

    /* Start Numerator */

    /* LDL-C Tests and HbA1c Tests Value Set */

    val ssdglucoseTestsHbA1cTestsValueSet = List(KpiConstants.glucoseTestsValueSet,KpiConstants.hba1cTestVal)
    val ssdglucoseTestsHbA1cTestsCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cptcat2CodeVal,KpiConstants.loincCodeVal)
    val hedisJoinedForglucoseTestsHbA1cTestsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ssdMeasureId, ssdglucoseTestsHbA1cTestsValueSet, ssdglucoseTestsHbA1cTestsCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val measrForglucoseTestsHbA1cTestsDf = UtilFunctions.mesurementYearFilter(hedisJoinedForglucoseTestsHbA1cTestsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)



    val finalNumeratorDf = measrForglucoseTestsHbA1cTestsDf.intersect(finalDinomenatorDf)

    finalNumeratorDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/

    val numeratorValueSet = ssdglucoseTestsHbA1cTestsValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, denominatorWithContErollDf, dinominatorExcl, finalNumeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)

    spark.sparkContext.stop()

  }


}

