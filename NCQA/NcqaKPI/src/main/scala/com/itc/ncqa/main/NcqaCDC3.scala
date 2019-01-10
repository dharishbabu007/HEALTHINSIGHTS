package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, to_date}

import scala.collection.JavaConversions._

object NcqaCDC3 {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC3")
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
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cdcMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Load the look up view based on the lob_name*/
    var lookUpTableDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Common filter (Removing the elements who has a gap of 45 days or 60 days)*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpTableDf.as("df2"), continuousEnrollDf.col(KpiConstants.memberskColName) === lookUpTableDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter("start_date is null").select("df1.*")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //ageFilterDf.orderBy("member_sk").show(50)
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">


    //<editor-fold desc="Dinominator1 (Step1)">

    /*Dinominator1(Step1) starts*/
    /*Loading refhedis data*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)

    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)


    /*Diabetes Valueset*/
    val diabtesValList = List(KpiConstants.diabetesVal)
    val joinedForDiabetesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, diabtesValList, primaryDiagCodeSystem)
    val measurementForDiab1Df = UtilFunctions.measurementYearFilter(joinedForDiabetesDf, "start_date", year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val diabetesValuesetDf = measurementForDiab1Df


    /*Telehealth Modifier and Telehealth Modifier valueset*/
    val teleHealAndModValList = List(KpiConstants.telehealthModifierVal, KpiConstants.telehealthPosVal)
    val teleHealAndModCodeSsytem = List(KpiConstants.modifierCodeVal, KpiConstants.posCodeVal)
    val joinedForTeleHealthDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,teleHealAndModValList,teleHealAndModCodeSsytem)
    val measurementForTeleHealthDf = UtilFunctions.measurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Acute Inpatient Value Set with a Diabetes Value Set and without Telehealth Modifier Value Set; Telehealth POS Value Set*/
    val dinominator1Sub1Df = measurementAcuteInpatDf.intersect(diabetesValuesetDf).except(measurementForTeleHealthDf)



    /*at least 2 Outpatient visit*/
    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,outPatValList,outPatCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*Telephone visit valueset*/
    val telephoneVisitValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTelephoneVisitDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,telephoneVisitValList,telephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.measurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val telephoneVisitAndDiabetesDf =  mesrForTelephoneVisitDf.intersect(diabetesValuesetDf)

    /*Online Assesment valueset*/
    val onlineVisitValList = List(KpiConstants.onlineAssesmentVal)
    val onlineVisitCodeSytsem = List(KpiConstants.cptCodeVal)
    val joinedForOnlineAssesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,onlineVisitValList,onlineVisitCodeSytsem)
    val mesrForOnlineAssesDf = UtilFunctions.measurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val onlineAssesAndDiabetesDf = mesrForOnlineAssesDf.intersect(diabetesValuesetDf)

    val twoOutPatAndTeleDf = twoOutPatDf.intersect(telephoneVisitAndDiabetesDf)
    /*at least 2 Outpatient visit,Telephone visit valueset, Online Assesment valueset*/
    val outPatTeleOnlineDf = twoOutPatAndTeleDf.intersect(onlineAssesAndDiabetesDf)


    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient without Telehealth*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    val acuteInPatwoTeleDf = twoNonAcutePatDf.except(measurementForTeleHealthDf)


    val dinominator2UnionDf = outPatTeleOnlineDf.union(twoObservationDf).union(twoEdVisitDf).union(acuteInPatwoTeleDf)
    /*atleast 2 visit with Outpatient  or Observation  or ED or Nonacute Inpatient  and has Diabetes */
    val dinominator1Sub2Df = dinominator2UnionDf.intersect(diabetesValuesetDf)
    //dinominator2Df.show()



    /*Diabetic Medication checking*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    //val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val diabeticMedicationValList = List(KpiConstants.diabetesMedicationVal)
    val joinedForDiabeticsmedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,diabeticMedicationValList)
    val dinominator1Sub3Df = UtilFunctions.measurementYearFilter(joinedForDiabeticsmedDf,KpiConstants.rxStartDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)


    /*Dinominator(Step1 Union)*/
    val dinominator1Df = dinominator1Sub1Df.union(dinominator1Sub2Df).union(dinominator1Sub3Df)
    /*Dinominator1(Step1) ends*/
    //</editor-fold>

    val dinominatorCalDf = dinominator1Df
    /*Join with age filter to create dinominatordf for output creation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorCalDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinominatorCalDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    dinominatorForKpiCalDf.show()

    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    //<editor-fold desc="dinominator Exclusion 1(Hospice)">

    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion2">

    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) ends*/

    //<editor-fold desc="66 years of age and older with frailty and advanced illness">

    /*(66 years of age and older with frailty and advanced illness) starts*/
    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,fralityValList,primaryDiagCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)


    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cdcMeasureId,advillValList,primaryDiagCodeSystem)
    val measrForAdvancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*union of atleast 2 outpatient visit, Observation visit,Ed visit,Non acute Visit*/
    val unionOfAllAtleastTwoVistDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)

    /*Members who has atleast 2 visits in any of(outpatient visit, Observation visit,Ed visit,Non acute Visit) and advanced ill*/
    val advancedIllAndTwoVistsDf = unionOfAllAtleastTwoVistDf.intersect(measrForAdvancedIllDf)

    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set starts*/
    val acuteAndAdvancedIllDf = measurementAcuteInpatDf.intersect(measrForAdvancedIllDf)
    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set ends*/

    /*dispensed dementia medication (Dementia Medications List) starts*/
    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val joinedForDemMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,dementiaMedValList)
    val MeasurementForDemMedDf = UtilFunctions.mesurementYearFilter(joinedForDemMedDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*dispensed dementia medication (Dementia Medications List) ends*/

    /*Members who has advanced Ill*/
    val advancedIllDf = advancedIllAndTwoVistsDf.union(acuteAndAdvancedIllDf).union(MeasurementForDemMedDf)

    /*(Members who has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDf = fralityDf.intersect(advancedIllDf)

    val age65OrMoreDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*(Members who has age 65 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    val dinoExcl2Df = fralityAndAdvIlDfAndAbove65Df
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    /*Dinominator3 (Step3) ends*/
    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 3 (Age filter  Exclusion)">

    val ageMore65Df = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 4 (CABG Exclusion)">

    val cabgValList = List(KpiConstants.cabgVal)
    val joinedForCabgDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, cabgValList, primaryDiagCodeSystem)
    val measurForCabgDf = UtilFunctions.measurementYearFilter(joinedForCabgDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 5 (PCI exclusion)">

    val pciValList = List(KpiConstants.pciVal)
    val joinedForPciDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, pciValList, primaryDiagCodeSystem)
    val measurForPciDf = UtilFunctions.measurementYearFilter(joinedForPciDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 6 (IVD condition)">

    val outpatValList = List(KpiConstants.outPatientVal)
    val outpatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, outpatValList, outPatCodeSystem)
    val measurForOutPatDf = UtilFunctions.measurementYearFilter(joinedForOutPatDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val)

    /*IVD EXCL*/
    val ivdValList = List(KpiConstants.ivdVal)
    val joinedForIvdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, ivdValList, primaryDiagCodeSystem)
    val measurForIvdDf = UtilFunctions.measurementYearFilter(joinedForIvdDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val)

    /*IVD and Outpatient*/
    val outPatAndIvdDf = measurForOutPatDf.intersect(measurForIvdDf)

    /*IVD and Telephone Visit*/
    val telephoneAndIvdDf = mesrForTelephoneVisitDf.intersect(measurForIvdDf)

    /*IVD and Online Assesment*/
    val onlineAndIvdDf = mesrForOnlineAssesDf.intersect(measurForIvdDf)

    //Acute Inpatient
    val acuteinpatValList = List(KpiConstants.acuteInpatientVal)
    val acuteinpatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForAcuteInPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, acuteinpatValList, acuteinpatCodeSystem)
    val measurForAcuteInPatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInPatDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val)


    /* Acute Inpatient and Ivd  without Telehealth*/
    val acuteInPatAndIvdwothDf = measurForAcuteInPatDf.intersect(measurForIvdDf).except(measurementForTeleHealthDf)

    /*union of joinOutPatAndIvdDf and joinAccuteInPatAndIvdDf*/
    val ivdDf = outPatAndIvdDf.union(telephoneAndIvdDf).union(onlineAndIvdDf).union(acuteInPatAndIvdwothDf)
    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 7 (Thoracic aortic aneurysm)">

    val thoaoaneValList = List(KpiConstants.thoraticAcriticVal)
    val joinedForThAoAnDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, thoaoaneValList, primaryDiagCodeSystem)
    val measurForThAoAnDf = UtilFunctions.measurementYearFilter(joinedForThAoAnDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val)

    /*join measurementOutPatExclDf with measurementThAoAnExclDf for the first condition*/
    val outPatAndThAoAnDf = measurForOutPatDf.intersect(measurForThAoAnDf).except(measurementForTeleHealthDf)

    /*join measurementAccuteInPatExclDf with measurementThAoAnExclDf for the second condition*/
    val accuteInAndThAoAnDf = measurForAcuteInPatDf.intersect(measurForThAoAnDf).except(measurementForTeleHealthDf)

    /*union of joinedOutPatAndThAoAnExclDf and joinedAccuteInAndThAoAnExclDf*/
    val thAoAnDf = outPatAndThAoAnDf.union(accuteInAndThAoAnDf)

    //</editor-fold>

    //<editor-fold desc="dinominator Exclusion 8">

    /*Chronic Heart Failure Exclusion*/
    val chrHeartFailValList = List(KpiConstants.chronicHeartFailureVal)
    val joinedForChrHeartFailDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, chrHeartFailValList, primaryDiagCodeSystem)
    val measurForChrHeartFailDf = UtilFunctions.mesurementYearFilter(joinedForChrHeartFailDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*MI*/
    val miValList = List(KpiConstants.miVal)
    val joinedForMiDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, miValList, primaryDiagCodeSystem)
    val measurForMiDf = UtilFunctions.measurementYearFilter(joinedForMiDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*ESRD AS Primary Diagnosis */
    val esrdValList = List(KpiConstants.esrdVal)
    val joinedForEsrdAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, esrdValList,primaryDiagCodeSystem)
    val measurForEsrdAsDiagDf = UtilFunctions.measurementYearFilter(joinedForEsrdAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*ESRD and  ESRD Obsolete as proceedure code*/
    val esrdCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.posCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val joinedForEsrdAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, esrdValList, esrdCodeSystem)
    val measurForEsrdAsProcDf = UtilFunctions.measurementYearFilter(joinedForEsrdAsProcDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*ESRD Exclusion by union ESRD AS Primary Diagnosis and ESRD and  ESRD Obsolete as proceedure code*/
    val esrdwoTelehealthDf = measurForEsrdAsDiagDf.union(measurForEsrdAsProcDf).except(measurementForTeleHealthDf)

    /*CKD Stage 4  Exclusion*/
    val ckdStage4ValList = List(KpiConstants.ckdStage4Val)
    val joinedForckdstage4Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, ckdStage4ValList, primaryDiagCodeSystem)
    val measurForckdstage4Df = UtilFunctions.measurementYearFilter(joinedForckdstage4Df, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Dementia Exclusion*/
    val dementiaValList = List(KpiConstants.dementiaVal, KpiConstants.fronDementiaVal)
    val joinedForDementiaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, dementiaValList, primaryDiagCodeSystem)
    val measurForDementiaDf = UtilFunctions.measurementYearFilter(joinedForDementiaDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Blindness Exclusion*/
    val blindnessValList = List(KpiConstants.blindnessVal)
    val joinedForBlindnessDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, blindnessValList, primaryDiagCodeSystem)
    val measurForBlindnessDf = UtilFunctions.measurementYearFilter(joinedForBlindnessDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)


    /* Lower extremity amputation Exclusion as primary diagnosis*/
    val leaValList = List(KpiConstants.lowerExtrAmputationVal)
    val joinedForLeaAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, leaValList, primaryDiagCodeSystem)
    val measurForLeaAsDiagDf = UtilFunctions.measurementYearFilter(joinedForLeaAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Lower extremity amputation Exclusion as Proceedure Code*/
    val leaCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForLeaAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, leaValList, leaCodeSystem)
    val measurForLeaAsProcDf = UtilFunctions.measurementYearFilter(joinedForLeaAsProcDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Lower extremity amputation condition*/
    val leaDf = measurForLeaAsDiagDf.union(measurForLeaAsProcDf)

    /*union of all Dinominator8 Exclusions*/
    val dinominator8ExclDf = measurForChrHeartFailDf.union(measurForMiDf).union(esrdwoTelehealthDf).union(measurForckdstage4Df).union(measurForDementiaDf).union(measurForBlindnessDf).union(leaDf)
    //</editor-fold>


    /*Union of all 8 Dinominator Exclusion Condition*/
    val dinominatorExclDf = hospiceDf.union(dinoExcl2Df).union(ageMore65Df).union(measurForCabgDf).union(measurForPciDf).union(ivdDf).union(thAoAnDf).union(dinominator8ExclDf)
    //unionOfAllDinominatorExclDf.show(50)

    /*CDC3 Dinominator for kpi calculation */
    val dinominatorAfterExclDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator (HbA1c test (HbA1c Tests Value Set))*/
    val hba1cTestValList = List(KpiConstants.hba1cTestVal)
    val hba1cTestCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cptcat2CodeVal, KpiConstants.loincCodeVal)
    val hedisJoinedForHba1cDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, hba1cTestValList,hba1cTestCodeSystem)
    val measurementForHba1cDf = UtilFunctions.measurementYearFilter(hedisJoinedForHba1cDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Numerator 2 (HbA1c poor control (<7.0%))*/
    val hba1clt7ValList = List(KpiConstants.hba1cLtSevenVal)
    val hba1clt7CodeSystem = List(KpiConstants.cptcat2CodeVal)
    val hedisJoinedForhba1clt7Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, hba1clt7ValList, hba1clt7CodeSystem)
    val measurForhba1clt7Df = UtilFunctions.measurementYearFilter(hedisJoinedForhba1clt7Df, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    val numeratorInterSectionDf = measurementForHba1cDf.intersect(measurForhba1clt7Df)
    val numeratorDf = numeratorInterSectionDf.intersect(dinominatorAfterExclDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*common output creation for facts_gaps_in_hedis table*/
    val numericReasonValueSet = KpiConstants.cdcNumerator1ValueSet ::: KpiConstants.cdc2Numerator2ValueSet
    val dinoExclReasonValueSet = KpiConstants.cdcDiabetesExclValueSet ::: KpiConstants.cdc3CabgValueSet ::: KpiConstants.cdc3PciValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numericReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cdc3MeasureId)

    /*creating empty dataframe for numEWxclDf*/
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)

    //</editor-fold>

    spark.sparkContext.stop()

  }

}
