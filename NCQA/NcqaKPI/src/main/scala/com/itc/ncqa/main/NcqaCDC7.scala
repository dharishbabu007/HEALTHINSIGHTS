package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, to_date}

import scala.collection.JavaConversions._

object NcqaCDC7 {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC7")
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
    /*Dinominator starts*/

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
    /*Dinominator ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()


    //<editor-fold desc="Dinominator Exclusion2">

    /*Dinominator3 (Step3) starts*/

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
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    /*Dinominator3 (Step3) ends*/
    //</editor-fold>

    //<editor-fold desc="Diabetes and Diabtes Exclsuion valueset">

    /*Find out the members who are not in diabetes valueset*/
    val membersWithoutDiabetesDf = dimMemberDf.select(KpiConstants.memberskColName).except(measurementForDiab1Df)
    /*Members who has diabetes exclusion valueset*/
    val diabExclValList = List(KpiConstants.diabetesExclusionVal)
    val hedisJoinedForDiabetesExclDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, diabExclValList, primaryDiagCodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.measurementYearFilter(hedisJoinedForDiabetesExclDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion3(Members who do not have a diagnosis of diabetes (Diabetes Value Set) and have (Diabetes Exclusions Value Set))*/
    val dinoExclusion2Df = membersWithoutDiabetesDf.intersect(measurementDiabetesExclDf)
    //</editor-fold>

    /*Union of Dinominator Exclusion*/
    val dinominatorExclDf = hospiceDf.union(fralityAndAdvIlDfAndAbove65Df).union(dinoExclusion2Df)

    /*Dinominator after Dinominator Exclusion*/
    val dinominatorAfterExclDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //</editor-fold>







    /*Numerator1 Calculation (Nephropathy screening or monitoring test)*/
    val urineprotieneValList = List(KpiConstants.urineProteinTestVal)
    val urineprotieneCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.loincCodeVal)
    val joinedForNephrScrnDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, urineprotieneValList, urineprotieneCodeSystem)
    val measurForNephrScrnDf = UtilFunctions.measurementYearFilter(joinedForNephrScrnDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)


    /*Numerator2 Calculation (Evidence of treatment for nephropathy or ACE/ARB therapy) as proceedure code*/
    val nephroTreatValList = List(KpiConstants.nephropathyTreatmentVal)
    val nephroTreatCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForNephrTreatAsPrDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, nephroTreatValList,nephroTreatCodeSystem)
    val measurForNephroTreatAsPrDf = UtilFunctions.measurementYearFilter(joinedForNephrTreatAsPrDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Numerator2 Calculation (Evidence of treatment for nephropathy or ACE/ARB therapy) as primary diagnosis*/
    val joinedForNephrTreatAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, nephroTreatValList, primaryDiagCodeSystem)
    val measurForNephrTreatAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForNephrTreatAsDiagDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select(KpiConstants.memberskColName)

    /*Numerator2 Calculation (Union of  measurementForNephropathyTreatmentAsPrDf and measurementForNephropathyTreatmentAsDiagDf)*/
    val NephropathyTreatmentDf = measurForNephroTreatAsPrDf.union(measurForNephrTreatAsDiagDf)


    /*Numerator3 Calculation (Evidence of stage 4 chronic kidney disease)*/
    val hedisJoinedForCkdStage4Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3CkdStage4ValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForCkdStage4Df = UtilFunctions.mesurementYearFilter(hedisJoinedForCkdStage4Df, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator4 Calculation (Evidence of ESRD) as proceedure code*/
    val hedisJoinedForEsrdAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7EsrdValueSet, KpiConstants.cdc3EsrdExclcodeSystem)
    val measurementForEsrdAsProDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsProDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator4 Calculation (Evidence of ESRD) as primary Diagnosis*/
    val hedisJoinedForEsrdAsDaigDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7EsrdValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForEsrdAsDaigDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsDaigDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator4 (union of measurementForEsrdAsProDf and measurementForEsrdAsDaigDf)*/
    val esrdDf = measurementForEsrdAsProDf.union(measurementForEsrdAsDaigDf)


    /*Numerator5 ,Evidence of kidney transplant as proceedure code */
    val hedisJoinedForKtAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7KtValueSet, KpiConstants.cdc7KtCodeSystem)
    val measurementForKtAsProDf = UtilFunctions.mesurementYearFilter(hedisJoinedForKtAsProDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator5 ,Evidence of kidney transplant as primary Diagnosis */
    val hedisJoinedForKtAsDaigDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc7KtValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForKtAsDaigDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsDaigDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator5 ,Evidence of kidney transplant(Union of measurementForKtAsProDf and measurementForKtAsDaigDf)*/
    val kidneyTranspalantDf = measurementForKtAsProDf.union(measurementForKtAsDaigDf).distinct()


    /*Numerator6 (visit with a nephrologist)*/
    val hedisJoinedForNephrologistDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc4ValueSetForFirstNumerator, KpiConstants.cdc4CodeSystemForFirstNumerator)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val joinedWithDimProviderDf = hedisJoinedForNephrologistDf.as("df1").join(dimProviderDf.as("df2"), hedisJoinedForNephrologistDf.col(KpiConstants.providerSkColName) === dimProviderDf.col(KpiConstants.providerSkColName), KpiConstants.innerJoinType).filter(dimProviderDf.col(KpiConstants.nephrologistColName).===("Y"))
    val measurementForNephrologistDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /*Numerator 7 (At least one ACE inhibitor or ARB dispensing event)*/
    /*val medValuesetForAceInhibitorDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"measure_id".===("CDC") && ($"medication_list".===("ACE Inhibitor/ARB Medications"))).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedForAceInhibitorDf = medValuesetForAceInhibitorDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeForAceInhibitorDf = startDateValAddedForAceInhibitorDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForAceInhibitorDf = UtilFunctions.mesurementYearFilter(dateTypeForAceInhibitorDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val aceInhibitorNumeratorDf = MeasurementForAceInhibitorDf.select("member_sk")*/


    /*Final Numerator (union of all the sub numerator conditions)*/
   /* val cdc7Numerator = measurementForNephropathyScreeningDf.union(NephropathyTreatmentDf).union(measurementForCkdStage4Df).union(esrdDf).union(kidneyTranspalantDf).union(measurementForNephrologistDf).union(aceInhibitorNumeratorDf)
    val cdc7numeratorDf = cdc7Numerator.intersect(cdc7DinominatorForKpiCalDf)
*/

    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.cdc7uptValueSet ::: KpiConstants.cdc7NtValueSet ::: KpiConstants.cdc3CkdStage4ValueSet ::: KpiConstants.cdc7EsrdValueSet ::: KpiConstants.cdc7KtValueSet
    val dinoExclReasonValueSet = KpiConstants.cdcDiabetesExclValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cdc7MeasureId)

    val numExclDf = spark.emptyDataFrame
   // val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, cdc7numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)

    spark.sparkContext.stop()


  }
}
