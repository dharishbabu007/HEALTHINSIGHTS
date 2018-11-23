package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, to_date}

import scala.collection.JavaConversions._

object NcqaSPC1A {

  def main(args: Array[String]): Unit = {



    /*Reading the program arguments*/
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

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASPDA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    import spark.implicits._


    var lookupTableDf = spark.emptyDataFrame


    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)


    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spcMeasureTitle)

    /*Loading view table based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 40 and 75 and female */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    val genderFilterDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.genderColName).===("F"))




    //<editor-fold desc="Dinominator">

    /*Dinominator Calculation starts*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    //<editor-fold desc="Dinominator step1">
    /*Dinominator step1 starts*/

    //<editor-fold desc="Dinominator Step1(Section1)">

    /*Dinominator step1_Section1(cardiovascular) starts*/
    /*Mi valueset for the last 2 years*/
    val joinedForMiValueSetDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcMiValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForMiDf = UtilFunctions.mesurementYearFilter(joinedForMiValueSetDf,KpiConstants.startDateColName,year,KpiConstants.measurementOneyearUpper,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Inpatient stay in last two years*/
    val joinedForInPatStayDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcInPatStayValueSet,KpiConstants.spcInPatStayCodeSystem)
    val measrForInPatStayDf = UtilFunctions.mesurementYearFilter(joinedForInPatStayDf,KpiConstants.dischargeDateColName,year,KpiConstants.measurementOneyearUpper,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Members who has MI valueset and Inpatient discharge in the measurement time.*/
    val miAndInPatStayDf = measrForMiDf.intersect(measrForInPatStayDf)


    /*CABG and PCI as Primary diagnosis*/
    val joinedForCabgAndPciAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcCabgAndPciValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForCabgAndPciAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForCabgAndPciAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementOneyearUpper,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*CABG,PCI,Other Revascularization as Proceedure code*/
    val joinedForCabgAndPciAsprocDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcCabgAndPciValueSet,KpiConstants.spcCabgAndPciCodeSytem)
    val measrForCabgAndPciAsprocDf = UtilFunctions.mesurementYearFilter(joinedForCabgAndPciAsprocDf,KpiConstants.startDateColName,year,KpiConstants.measurementOneyearUpper,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*members who has CABG and PCI and Other Revascularization as valueset during the measurement time*/
    val cabgAndPciDf = measrForCabgAndPciAsDiagDf.union(measrForCabgAndPciAsprocDf)

    /*Dinominator step1 (Members who has cardiovascular)*/
    val cardiovascularDf = miAndInPatStayDf.union(cabgAndPciDf)
    /*Dinominator step1_Section1(cardiovascular) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Step1(Section2)">

    /*Dinominator Step1_Section2(ischemic vascular disease (IVD)) starts*/
    /*IVD valueset*/
    val joinedForIvdDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcIvdValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForIvdDf = UtilFunctions.mesurementYearFilter(joinedForIvdDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Members who has atleast one outpatient visist*/
    val joinedForTwoOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcOutPatientValueSet,KpiConstants.spcOutPatientCodeSystem)
    val measrForOneOutPatDf = UtilFunctions.mesurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*members who has ivd and outpatient*/
    val ivdAndOutPatientDf = measrForOneOutPatDf.intersect(measrForIvdDf)

    /*Members who has atleast one Telephone visit*/
    val joinedForTelephoneVisitDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcTelephoneVisitValueSet,KpiConstants.spcTelephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.mesurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*members who has ivd and Telephone visit*/
    val ivdAndTelephoneVisitDf = mesrForTelephoneVisitDf.intersect(measrForIvdDf)

    /*members who has atleast one Online Assesment*/
    val joinedForOnlineAssesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcOnlineAssesValueSet,KpiConstants.spcOnlineAssesCodeSystem)
    val mesrForOnlineAssesDf = UtilFunctions.mesurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*members who has ivd and Online Assesment*/
    val ivdAndOnlineAssesDf = mesrForOnlineAssesDf.intersect(measrForIvdDf)

    /*members who has atleast one acute inpatient*/
    val joinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcAcuteInpatientValueSet,KpiConstants.spcAcuteInpatientCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Telehealth Modifier valueset*/
    val joinedForTeleHealthDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcTeleHealthValueSet,KpiConstants.spcTeleHealthCodeSystem)
    val measurementForTeleHealthDf = UtilFunctions.mesurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*members who has ivd and acute inpatient and without Telehealth*/
    val ivdAndAcuteInPatWithoutTeleHealthDf = measurementAcuteInpatDf.intersect(measrForIvdDf).except(measurementForTeleHealthDf)

    /*Dinominator Step1_Section2(ischemic vascular disease (IVD))*/
    val ivdDf = ivdAndOutPatientDf.union(ivdAndTelephoneVisitDf).union(ivdAndOnlineAssesDf).union(ivdAndAcuteInPatWithoutTeleHealthDf)
    /*Dinominator Step1_Section2(ischemic vascular disease (IVD)) ends*/
    //</editor-fold>

    /*Dinominator step1*/
    val dinominator1Df = cardiovascularDf.union(ivdDf)
    /*Dinominator step1 ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator step2">

    /*Dinominator step2(Exclusion1) starts*/
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)

    /*Dinominator step2 section1(Female members with a diagnosis of pregnancy) starts*/
    val joinedForPregnancyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcPregnancyValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForPregnancyDf = UtilFunctions.mesurementYearFilter(joinedForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*Dinominator step2 section1(Female members with a diagnosis of pregnancy) ends*/

    /*Dinominator step2 section2(In vitro fertilization (IVF Value Set)) starts*/
    val joinedForIvfDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcIvfValueSet,KpiConstants.spcIvfCodeSystem)
    val measrForIvfDf = UtilFunctions.mesurementYearFilter(joinedForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*Dinominator step2 section2(In vitro fertilization (IVF Value Set)) ends*/

    /*Dinominator step2 section3(Dispensed at least one prescription for clomiphene (Estrogen Agonists Medications List)) starts*/
    val joinedForClomipheneDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"medication_list".isin(KpiConstants.spdEstroAgonistsMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForClomipheneDf = joinedForClomipheneDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForClomipheneDf = startDateValAddedDfForClomipheneDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForClomipheneDf = UtilFunctions.mesurementYearFilter(dateTypeDfForClomipheneDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*Dinominator step2 section3(Dispensed at least one prescription for clomiphene (Estrogen Agonists Medications List)) ends*/

    /*Dinominator step2 section4(ESRD (ESRD Value Set) without (Telehealth Modifier Value Set; Telehealth POS Value Set)) starts*/
    /*ESRD as Primary Diagnosis*/
    val joinedForEsrdAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcEsrdValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val mesrForEsrdAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForEsrdAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*ESRD as proceedure code*/
    val joinedForEsrdAsProcDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcEsrdValueSet,KpiConstants.spcEsrdCodeSystem)
    val measrForEsrdAsProcDf = UtilFunctions.mesurementYearFilter(joinedForEsrdAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val esrdUnionDf = mesrForEsrdAsDiagDf.union(measrForEsrdAsProcDf)

    /*Members who has ESRD without Telehealth*/
    val esrdWithoutTeleHealthDf = esrdUnionDf.except(measurementForTeleHealthDf)
    /*Dinominator step2 section4(ESRD (ESRD Value Set) without (Telehealth Modifier Value Set; Telehealth POS Value Set)) ends*/

    /*Dinominator step2 section5(Cirrhosis (Cirrhosis Value Set)) starts*/
    val joinedForCirrhosisDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcCirrhosisValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForCirrhosisDf = UtilFunctions.mesurementYearFilter(joinedForCirrhosisDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*Dinominator step2 section5(Cirrhosis (Cirrhosis Value Set)) ends*/

    /*Dinominator step2 section6(Myalgia, myositis, myopathy or rhabdomyolysis (Muscular Pain and Disease Value Set)) starts*/
    val joinedForMusPainDisDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcMusPainDisValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForMusPainDisDf = UtilFunctions.mesurementYearFilter(joinedForMusPainDisDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*Dinominator step2 section6(Myalgia, myositis, myopathy or rhabdomyolysis (Muscular Pain and Disease Value Set)) ends*/

    /*Dinominator step2(Exclusion1)Union of all sub types in the step2*/
    val dinominator2Df = measrForPregnancyDf.union(measrForIvfDf).union(MeasurementForClomipheneDf).union(esrdWithoutTeleHealthDf).union(measrForCirrhosisDf).union(measrForMusPainDisDf)
    /*Dinominator step2(Exclusion1) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator step3">

    /*Dinominator Step3 starts*/

    /*Dinominator step3 section1(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*Dinominator step3 section1(Enrolled in an Institutional SNP (I-SNP)) ends*/


    //<editor-fold desc="Dinominator step3 Section2">

    /*Dinominator step3 section2 (66 years of age or older with frailty and advanced illness) starts*/
    /*Frality As Primary Diagnosis*/
    val joinedForFralityAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcFralityValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val joinedForFralityAsProcDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcFralityValueSet,KpiConstants.spcFralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.mesurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)


    /*Advanced Illness valueset*/
    val joinedForAdvancedIllDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcAdvancedIllValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measrForAdvancedIllDf = UtilFunctions.mesurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)


    /*at least 2 Outpatient visit*/
    val measrForTwoOutPatDf = UtilFunctions.mesurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val joinedForTwoObservationDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcObservationValueSet,KpiConstants.spcObservationCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.mesurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val joinedForTwoEdVisistsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcEdVisitValueSet,KpiConstants.spcEdVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.mesurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient*/
    val joinedForTwoNonAcutePatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcNonAcutePatValueSet,KpiConstants.spcNonAcutePatCodeSystem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.mesurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)


    /*union of atleast 2 outpatient visit, Observation visit,Ed visit,Non acute Visit*/
    val unionOfAllAtleastTwoVistDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)

    /*Members who has atleast 2 visits in any of(outpatient visit, Observation visit,Ed visit,Non acute Visit) and advanced ill*/
    val advancedIllAndTwoVistsDf = unionOfAllAtleastTwoVistDf.intersect(measrForAdvancedIllDf)

    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set starts*/
    val acuteAndAdvancedIllDf = measurementAcuteInpatDf.intersect(measrForAdvancedIllDf)
    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set ends*/

    /*dispensed dementia medication (Dementia Medications List) starts*/
    val joinedForDemMedDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"medication_list".isin(KpiConstants.spcDementiaMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForDemMedDf = joinedForDemMedDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForDemMedDf = startDateValAddedDfForDemMedDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForDemMedDf = UtilFunctions.mesurementYearFilter(dateTypeDfForDemMedDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*dispensed dementia medication (Dementia Medications List) ends*/

    /*Members who has advanced Ill*/
    val advancedIllDf = advancedIllAndTwoVistsDf.union(acuteAndAdvancedIllDf).union(MeasurementForDemMedDf)

    /*Dinominator Exclusion10(Members who has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDf = fralityDf.union(advancedIllDf)

    val age65OrMoreDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*Dinominator step3(Members who has age 65 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*Dinominator step3 section2 (66 years of age or older with frailty and advanced illness) ends*/
    //</editor-fold>

    /*union of step3 section1 and section2*/
    val dinominator3Df = fralityAndAdvIlDfAndAbove65Df
    /*Dinominator Step3 ends*/
    //</editor-fold>

    /*union of step2 and step3*/
    val exclusionForDinoDf = dinominator2Df.union(dinominator3Df)

    /*dinominator dataframe */
    val dinominatorUnionDf = dinominator1Df.except(exclusionForDinoDf)

    /*Dinominator output for calculation*/
    val dinominatorDf = genderFilterDf.as("df1").join(dinominatorUnionDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinominatorUnionDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion">

    /*Dinominator Exclusion starts*/
    /*Hospice Exclusion*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName)
    dinominatorExclDf.show()
    val dinominatorAfterExclDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    /*Dinominator Exclusion ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator">

    /*Numerator Calculation starts*/
    val joinedForHmismDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"medication_list".isin(KpiConstants.spcHmismMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForHmismDf = joinedForHmismDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForHmismDf = startDateValAddedDfForHmismDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForHmismDf = UtilFunctions.mesurementYearFilter(dateTypeDfForHmismDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("member_sk")
    val numeratorDf = MeasurementForHmismDf.intersect(dinominatorAfterExclDf)
    numeratorDf.show()
    /*Numerator Calculation ends*/
    //</editor-fold>


    //<editor-fold desc="output to fact_hedis_gaps_in_care">

    /*Common output format (data to fact_hedis_gaps_in_care) starts*/
    /*create the reason valueset for output data*/
    val numeratorValueSet = KpiConstants.spdHmismMedicationListVal
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.spc1aMeasureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    /*Common output format (data to fact_hedis_gaps_in_care) ends*/
    //</editor-fold>

    //<editor-fold desc="Output to fact_hedis_qms starts">

    /*Data populating to fact_hedis_qms starts*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.spcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    /*Data populating to fact_hedis_qms ends*/
    //</editor-fold>

    spark.sparkContext.stop()

  }
}
