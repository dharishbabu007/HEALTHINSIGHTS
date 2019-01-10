package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object NcqaCBP {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACBP")
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

    //<editor-fold desc="Initial Join, Continous Enrollment,Allowable Gap and Age filter">

    /*Initial join function call to prepare the data for common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cbpMeasureTitle)
    //initialJoinedDf.show(50)

    /*Continous enrollment calculation*/
    val contEnrollEndDate = year + "-12-31"
    val contEnrollStrtDate = year + "-01-01"
    val continiousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(initialJoinedDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate)))

    /*Allowable gap filter* based on the lob name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = continiousEnrollDf.as("df1").join(lookUpDf.as("df2"), continiousEnrollDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*Age Filter(18–85) */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age85Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    //<editor-fold desc="Dinominator1(Outpatient with or without Telelhealth Modifier and Essential Hyper Tension)">

    //<editor-fold desc="Essential Hyper Tension">

    /*Essential Hyper Tension As primary diagnosis*/
    val essHypTenValList = List(KpiConstants.essentialHyptenVal)
    val primaryDiagCodeVal = List(KpiConstants.icdCodeVal)
    val joinForEssHypTenAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, essHypTenValList, primaryDiagCodeVal)
    val measurForEssHypTenAsDiagDf = UtilFunctions.measurementYearFilter(joinForEssHypTenAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Essential Hyper Tension As proceedural code*/
    val eessHypTenCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.modifierCodeVal)
    val joinForEssHypTenAsProDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, essHypTenValList, eessHypTenCodeSystem)
    val measurForEssHypTenAsProDf = UtilFunctions.measurementYearFilter(joinForEssHypTenAsProDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    val essentialHypTenDf = measurForEssHypTenAsDiagDf.union(measurForEssHypTenAsProDf)
    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV  with or without Telehealth Modifier ">

    val outPatwoUbrevValList = List(KpiConstants.outpatwoUbrevVal,KpiConstants.telehealthModifierVal)
    val outPatwoUbrevCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal,KpiConstants.modifierCodeVal)
    val joinForOutpatWoUbrevDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf,KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,outPatwoUbrevValList,outPatwoUbrevCodeSystem)
    val measrForOutpatWoUbrevDf = UtilFunctions.measurementYearFilter(joinForOutpatWoUbrevDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    val dinominator1Df = measrForOutpatWoUbrevDf.intersect(essentialHypTenDf)
    //</editor-fold>

    //<editor-fold desc="Dinominator2(Essential Hyper Tension and Telephone visit)">

    val telephoneVistValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVistCodeSystem = List(KpiConstants.cptCodeVal)
    val joinFortelephoneVistDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, telephoneVistValList,telephoneVistCodeSystem)
    val measurFortelephoneVistDf = UtilFunctions.measurementYearFilter(joinFortelephoneVistDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val dinominator2Df = measurFortelephoneVistDf.intersect(essentialHypTenDf)
    //</editor-fold>

    //<editor-fold desc="Dinominator3(Essential Hyper Tension and Online Assesment)">

    val onlineassesValList = List(KpiConstants.onlineAssesmentVal)
    val onlineassesCodeSystem = List(KpiConstants.cptCodeVal)
    val joinForonlineassesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, onlineassesValList, onlineassesCodeSystem)
    val measurForonlineassesDf = UtilFunctions.measurementYearFilter(joinForonlineassesDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val dinominator3Df = measurForonlineassesDf.intersect(essentialHypTenDf)
    //</editor-fold>

    val dinominatorUnionDf = dinominator1Df.union(dinominator2Df).union(dinominator3Df)
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === dinominatorUnionDf.col(KpiConstants.memberskColName)).select("df1.member_sk")
    val dinominatorForKpiDf = dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    //<editor-fold desc="Dinominator Exclusion1">


    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) ends*/

    //<editor-fold desc="66 years of age and older with frailty and advanced illness">

    /*(66 years of age and older with frailty and advanced illness) starts*/
    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,fralityValList,primaryDiagCodeVal)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)


    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,advillValList,primaryDiagCodeVal)
    val measrForAdvancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*union of atleast 2 outpatient visit, Observation visit,Ed visit,Non acute Visit*/
    /*at least 2 Outpatient visit*/
    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,outPatValList,outPatCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient without Telehealth*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    val unionOfAllAtleastTwoVistDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)

    /*Members who has atleast 2 visits in any of(outpatient visit, Observation visit,Ed visit,Non acute Visit) and advanced ill*/
    val advancedIllAndTwoVistsDf = unionOfAllAtleastTwoVistDf.intersect(measrForAdvancedIllDf)

    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set starts*/
    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val acuteAndAdvancedIllDf = measurementAcuteInpatDf.intersect(measrForAdvancedIllDf)
    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set ends*/

    /*dispensed dementia medication (Dementia Medications List) starts*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val joinedForDemMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,dementiaMedValList)
    val measurementForDemMedDf = UtilFunctions.mesurementYearFilter(joinedForDemMedDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*dispensed dementia medication (Dementia Medications List) ends*/

    /*Members who has advanced Ill*/
    val advancedIllDf = advancedIllAndTwoVistsDf.union(acuteAndAdvancedIllDf).union(measurementForDemMedDf)

    /*(Members who has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDf = fralityDf.intersect(advancedIllDf)

    val age65OrMoreDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age66Val, KpiConstants.age80Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*(Members who has age 66 to 80 and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    val dinominatorExclusion1Df = fralityAndAdvIlDfAndAbove65Df
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    /*Dinominator3 (Step3) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Eclusion2(Optional)">

    //<editor-fold desc="ESRD, ESRD Obsolent and kidney transplant">

    /*Exclusions for primaryDiagnosisCodeSystem with valueset "ESRD","Kidney Transplant" */
    val esrdKidneyTrdiagValList = List(KpiConstants.esrdVal, KpiConstants.kidneyTransplantVal)
    val joinForesrdKidneyTrdiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, esrdKidneyTrdiagValList, primaryDiagCodeVal)
    val mesurForesrdKidneyTrdiagDf = UtilFunctions.measurementYearFilter(joinForesrdKidneyTrdiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Exclusions for proceedureCodeSystem with valueset "ESRD","ESRD Obsolete","Kidney Transplant" */
    val esrdKidneyTrProcValList = List(KpiConstants.esrdVal, KpiConstants.esrdObsoleteVal, KpiConstants.kidneyTransplantVal)
    val esrdKidneyTrProcCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.posCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val joinForesrdKidneyTrProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, esrdKidneyTrProcValList, esrdKidneyTrProcCodeSystem)
    val measurForesrdKidneyTrProcDf = UtilFunctions.measurementYearFilter(joinForesrdKidneyTrProcDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    val esrdKidTransDf = mesurForesrdKidneyTrdiagDf.union(measurForesrdKidneyTrProcDf)
    //</editor-fold>

    //<editor-fold desc="pregnancy Exclusion">

    val pregnancyValList = List(KpiConstants.pregnancyVal)
    val joinedForPregnancyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,pregnancyValList,primaryDiagCodeVal)
    val measurForPregnancyDf = UtilFunctions.measurementYearFilter(joinedForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Non acute Inpatient">

    val nonAcuteInPatValList = List(KpiConstants.inpatientStayVal, KpiConstants.nonacuteInPatStayVal)
    val nonAcuteInPatCodeSystem = List(KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val joinFornonAcuteInPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, nonAcuteInPatValList, nonAcuteInPatCodeSystem)
    val measurnonAcuteInPatDf = UtilFunctions.measurementYearFilter(joinFornonAcuteInPatDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    val dinominatorExclusion2Df = 	esrdKidTransDf.union(measurForPregnancyDf).union(measurnonAcuteInPatDf)
    //</editor-fold>

    val dinominatorExclDf = dinominatorExclusion1Df.union(dinominatorExclusion2Df)
    val dinominatorAfterExclusionDf = dinominatorForKpiDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc=" Outpatient Without UBREV,Nonacute Inpatient,Remote Blood Pressure Monitoring valueset">

    val outpatnonacuterebpValList = List(KpiConstants.outpatwoUbrevVal, KpiConstants.nonAcuteInPatientVal,KpiConstants.remotebpmVal)
    val outpatnonacuterebpCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinForoutpatnonacuterebpDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,outpatnonacuterebpValList, outpatnonacuterebpCodeSystem)
    val measurForoutpatnonacuterebpDf = UtilFunctions.measurementYearFilter(joinForoutpatnonacuterebpDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="bpReading (Systolic Less Than 140, Diastolic Less Than 80,Diastolic 80–89 valueset)">

    val bpReadingValList = List(KpiConstants.systolicLt140Val, KpiConstants.diastolicLt80Val, KpiConstants.diastolicBtwn8090Val)
    val bpReadingCodeSystem = List(KpiConstants.cptCatIIVal, KpiConstants.cptCatIIVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal, KpiConstants.posCodeVal)
    val joinForbpReadingDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, bpReadingValList, bpReadingCodeSystem)
    val measurForbpReadingDf = UtilFunctions.measurementYearFilter(joinForbpReadingDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName, KpiConstants.startDateColName)
    //</editor-fold>

    /*Mmeber_sk and start_date for members who has both bpReading and outpatnonacuterebp*/
    val bpReadingAndoutpatnonacuterebpDf = measurForoutpatnonacuterebpDf.as("df1").join(measurForbpReadingDf.as("df2"),measurForoutpatnonacuterebpDf.col(KpiConstants.memberskColName) === measurForbpReadingDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType)
                                                                                         .select("df2.member_sk","df2.start_date")

    /*member_sk and most recentbp reding */
    val mostRecentBpreadingDf = bpReadingAndoutpatnonacuterebpDf.groupBy(KpiConstants.memberskColName).agg(max(bpReadingAndoutpatnonacuterebpDf.col(KpiConstants.startDateColName)).alias(KpiConstants.mrbpreadingColName))

    /*Second diagnosis of Essential Hyper tension*/
    val measurForEssHypTenDf = UtilFunctions.measurementYearFilter(joinForEssHypTenAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val secondHyperTenDiagDf = measurForEssHypTenDf.withColumn("rank", row_number().over(Window.partitionBy($"member_sk").orderBy($"start_date".asc))).filter($"rank"===(2))

    val numCondDf = mostRecentBpreadingDf.as("df1").join(secondHyperTenDiagDf.as("df2"),mostRecentBpreadingDf.col(KpiConstants.memberskColName) === secondHyperTenDiagDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType)
                                                          .filter(mostRecentBpreadingDf.col(KpiConstants.startDateColName).>=(secondHyperTenDiagDf.col(KpiConstants.startDateColName))).select("df1.member_sk")

    val numeratorDf = numCondDf.intersect(dinominatorAfterExclusionDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = outpatnonacuterebpValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)


    val numExclDf = spark.emptyDataFrame


    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()

  }

}
