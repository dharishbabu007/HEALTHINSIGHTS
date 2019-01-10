package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

object NcqaSPDA {

  def main(args: Array[String]): Unit = {


    //<editor-fold desc="Reading Program Arguments and Spark Session object creation">

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASPDA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading the required table to Memory">

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
    //</editor-fold>

    //<editor-fold desc="Initial Join,Continous Enrollment, allowable Gap filter and Age filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spdMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year.toInt - 1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Loading view table based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 40 and 75*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
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
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)


    /*Diabetes Valueset*/
    val diabtesValList = List(KpiConstants.diabetesVal)
    val joinedForDiabetesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, diabtesValList, primaryDiagCodeSystem)
    val measurementForDiab1Df = UtilFunctions.measurementYearFilter(joinedForDiabetesDf, "start_date", year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val diabetesValuesetDf = measurementForDiab1Df


    /*Telehealth Modifier and Telehealth Modifier valueset*/
    val teleHealAndModValList = List(KpiConstants.telehealthModifierVal, KpiConstants.telehealthPosVal)
    val teleHealAndModCodeSsytem = List(KpiConstants.modifierCodeVal, KpiConstants.posCodeVal)
    val joinedForTeleHealthDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,teleHealAndModValList,teleHealAndModCodeSsytem)
    val measurementForTeleHealthDf = UtilFunctions.measurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Acute Inpatient Value Set with a Diabetes Value Set and without Telehealth Modifier Value Set; Telehealth POS Value Set*/
    val dinominator1Sub1Df = measurementAcuteInpatDf.intersect(diabetesValuesetDf).except(measurementForTeleHealthDf)



    /*at least 2 Outpatient visit*/
    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,outPatValList,outPatCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*Telephone visit valueset*/
    val telephoneVisitValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTelephoneVisitDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,telephoneVisitValList,telephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.measurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val telephoneVisitAndDiabetesDf =  mesrForTelephoneVisitDf.intersect(diabetesValuesetDf)

    /*Online Assesment valueset*/
    val onlineVisitValList = List(KpiConstants.onlineAssesmentVal)
    val onlineVisitCodeSytsem = List(KpiConstants.cptCodeVal)
    val joinedForOnlineAssesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,onlineVisitValList,onlineVisitCodeSytsem)
    val mesrForOnlineAssesDf = UtilFunctions.measurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val onlineAssesAndDiabetesDf = mesrForOnlineAssesDf.intersect(diabetesValuesetDf)

    val twoOutPatAndTeleDf = twoOutPatDf.intersect(telephoneVisitAndDiabetesDf)
    /*at least 2 Outpatient visit,Telephone visit valueset, Online Assesment valueset*/
    val outPatTeleOnlineDf = twoOutPatAndTeleDf.intersect(onlineAssesAndDiabetesDf)


    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient without Telehealth*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
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

    //<editor-fold desc="Dinominator2(Step2)">

    //<editor-fold desc="Cardiovascular">

    /*Mi valueset for the previous year*/
    val miValList = List(KpiConstants.miVal)
    val joinedForMiValueSetDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,miValList,primaryDiagCodeSystem)
    val measrForMiDf = UtilFunctions.measurementYearFilter(joinedForMiValueSetDf,KpiConstants.startDateColName,year,KpiConstants.measurement1Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Inpatient stay for the previous year*/
    val inPatStayValList = List(KpiConstants.inPatientStayVal)
    val inPatStayCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinedForInPatStayDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,inPatStayValList,inPatStayCodeSystem)
    val measrForInPatStayDf = UtilFunctions.measurementYearFilter(joinedForInPatStayDf,KpiConstants.dischargeDateColName,year,KpiConstants.measurement1Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Members who has MI valueset and Inpatient discharge in the measurement time.*/
    val miAndInPatStayDf = measrForMiDf.intersect(measrForInPatStayDf)


    /*CABG and PCI as Primary diagnosis*/
    val cabgAndPciValList = List(KpiConstants.cabgVal, KpiConstants.pciVal)
    val joinedForCabgAndPciAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,cabgAndPciValList,primaryDiagCodeSystem)
    val measrForCabgAndPciAsDiagDf = UtilFunctions.measurementYearFilter(joinedForCabgAndPciAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement1Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*CABG,PCI,Other Revascularization as Proceedure code*/
    val cabgPciRevascValList = List(KpiConstants.cabgVal,KpiConstants.pciVal,KpiConstants.otherRevascularizationVal)
    val cabgAndPciReVascCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForCabgAndPciAsprocDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,cabgPciRevascValList,cabgAndPciReVascCodeSystem)
    val measrForCabgAndPciAsprocDf = UtilFunctions.measurementYearFilter(joinedForCabgAndPciAsprocDf,KpiConstants.startDateColName,year,KpiConstants.measurement1Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*members who has CABG and PCI and Other Revascularization as valueset during the measurement time*/
    val cabgAndPciDf = measrForCabgAndPciAsDiagDf.union(measrForCabgAndPciAsprocDf)



    /*Dinominator Exclusion1 (Members who has cardiovascular)*/
    val cardiovascularDf = miAndInPatStayDf.union(cabgAndPciDf)
    /*Dinominator Exclusion1(cardiovascular) ends*/
    //</editor-fold>

    //<editor-fold desc="Ivd">

    /*IVD valueset*/
    val ivdValList = List(KpiConstants.ivdVal)
    val joinedForIvdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,ivdValList,primaryDiagCodeSystem)
    val measrForIvdDf = UtilFunctions.measurementYearFilter(joinedForIvdDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*members who has ivd and outpatient*/
    val ivdAndOutPatientDf = measrForTwoOutPatDf.select(KpiConstants.memberskColName).intersect(measrForIvdDf)

    /*members who has ivd and Telephone visit*/
    val ivdAndTelephoneVisitDf = mesrForTelephoneVisitDf.intersect(measrForIvdDf)

    /*members who has ivd and Online Assesment*/
    val ivdAndOnlineAssesDf = mesrForOnlineAssesDf.intersect(measrForIvdDf)

    /*members who has ivd and acute inpatient and without Telehealth*/
    val ivdAndAcuteInPatWithoutTeleHealthDf = measurementAcuteInpatDf.intersect(measrForIvdDf).except(measurementForTeleHealthDf)

    /*Dinominator Exclusion2*/
    val ivdDf = ivdAndOutPatientDf.union(ivdAndTelephoneVisitDf).union(ivdAndOnlineAssesDf).union(ivdAndAcuteInPatWithoutTeleHealthDf)
    //</editor-fold>

    //<editor-fold desc="Pregnancy">

    /*Dinominator Exclusion3(Female members with a diagnosis of pregnancy) starts*/
    val pregnancyValList = List(KpiConstants.pregnancyVal)
    val joinedForPregnancyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,pregnancyValList,primaryDiagCodeSystem)
    val measrForPregnancyDf = UtilFunctions.measurementYearFilter(joinedForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion3(Female members with a diagnosis of pregnancy) ends*/
    //</editor-fold>

    //<editor-fold desc="Ivf">

    /*Dinominator Exclusion4(In vitro fertilization (IVF Value Set)) starts*/
    val ivfValList = List(KpiConstants.ivfVal)
    val ivfCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForIvfDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,ivfValList,ivfCodeSystem)
    val measrForIvfDf = UtilFunctions.measurementYearFilter(joinedForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion4(In vitro fertilization (IVF Value Set)) ends*/
    //</editor-fold>

    //<editor-fold desc="Estrogen Agonists Medications">

    /*Dinominator Exclusion5(Dispensed at least one prescription for clomiphene (Estrogen Agonists Medications List)) starts*/
    val estrogenAgnoMedValList = List(KpiConstants.estrogenAgonistsMediVal)
    val joinedForClomipheneDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,estrogenAgnoMedValList)
    val MeasurementForClomipheneDf = UtilFunctions.measurementYearFilter(joinedForClomipheneDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion5(Dispensed at least one prescription for clomiphene (Estrogen Agonists Medications List)) ends*/
    //</editor-fold>

    //<editor-fold desc="ESRD without Telehealth">

    /*Dinominator Exclusion6(ESRD (ESRD Value Set) without (Telehealth Modifier Value Set; Telehealth POS Value Set)) starts*/
    /*ESRD as Primary Diagnosis*/
    val esrdValList = List(KpiConstants.esrdVal)
    val joinedForEsrdAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,esrdValList,primaryDiagCodeSystem)
    val mesrForEsrdAsDiagDf = UtilFunctions.measurementYearFilter(joinedForEsrdAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*ESRD as proceedure code*/
    val esrdCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.posCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.ubtobCodeVal)
    val joinedForEsrdAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,esrdValList,esrdCodeSystem)
    val measrForEsrdAsProcDf = UtilFunctions.measurementYearFilter(joinedForEsrdAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    val esrdUnionDf = mesrForEsrdAsDiagDf.union(measrForEsrdAsProcDf)
    /*Members who has ESRD without Telehealth*/
    val esrdWithoutTeleHealthDf = esrdUnionDf.except(measurementForTeleHealthDf)
    /*Dinominator Exclusion6(ESRD (ESRD Value Set) without (Telehealth Modifier Value Set; Telehealth POS Value Set)) ends*/
    //</editor-fold>

    //<editor-fold desc="Cirrhosis">

    /*(Cirrhosis (Cirrhosis Value Set)) starts*/
    val cirrhosisValList = List(KpiConstants.cirrhossisVal)
    val joinedForCirrhosisDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,cirrhosisValList,primaryDiagCodeSystem)
    val measrForCirrhosisDf = UtilFunctions.measurementYearFilter(joinedForCirrhosisDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*(Cirrhosis (Cirrhosis Value Set)) ends*/
    //</editor-fold>

    //<editor-fold desc="Myalgia, myositis, myopathy or rhabdomyolysis">

    /*(Myalgia, myositis, myopathy or rhabdomyolysis (Muscular Pain and Disease Value Set)) starts*/
    val muscuAndDisValList = List(KpiConstants.muscularPainAndDiseaseval)
    val joinedForMusPainDisDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,muscuAndDisValList,primaryDiagCodeSystem)
    val measrForMusPainDisDf = UtilFunctions.measurementYearFilter(joinedForMusPainDisDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select(KpiConstants.memberskColName)
    /*(Myalgia, myositis, myopathy or rhabdomyolysis (Muscular Pain and Disease Value Set)) ends*/
    //</editor-fold>

    /*Dinominator2(Step2) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator3 (Step3)">

    /*Dinominator3 (Step3) starts*/

    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) ends*/

    //<editor-fold desc="66 years of age and older with frailty and advanced illness">

    /*(66 years of age and older with frailty and advanced illness) starts*/
    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,fralityValList,primaryDiagCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)


    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,advillValList,primaryDiagCodeSystem)
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

    /*Union of step2 dataframes and step3 dataframes which we have to exclude in dinominator calculation*/
    val dinoexclUnionDf = cardiovascularDf.union(ivdDf).union(measrForPregnancyDf).union(measrForIvfDf).union(MeasurementForClomipheneDf).union(esrdWithoutTeleHealthDf).union(measrForCirrhosisDf).union(measrForMusPainDisDf).union(fralityAndAdvIlDfAndAbove65Df)

    /*Dinominator after exclude step2,step3 members*/
    val dinominatorCalDf = dinominator1Df.except(dinoexclUnionDf)

    /*Join with age filter to create dinominatordf for output creation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorCalDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinominatorCalDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    dinominatorForKpiCalDf.show()
    /*Dinominator ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion">

    /*Hospice Exclusion*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf)
    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName)
    //dinominatorExclDf.show()
    val dinominatorAfterExclDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator">

    /*High and Moderate-Intensity Statin Medications List; Low-Intensity Statin Medications List*/
    val hmiliStatinValList = List(KpiConstants.highAndModerateStatinMedVal, KpiConstants.lowStatinMedVal)
    val joinedForHmismDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark, factRxClaimsDf, ref_medvaluesetDf, KpiConstants.spdaMeasureId,hmiliStatinValList)
    val MeasurementForHmismDf = UtilFunctions.measurementYearFilter(joinedForHmismDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val).select("member_sk")
    val numeratorDf = MeasurementForHmismDf.intersect(dinominatorAfterExclDf)
    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care) starts*/
    /*create the reason valueset for output data*/
    val numeratorValueSet = hmiliStatinValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source, measureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    /*Common output format (data to fact_hedis_gaps_in_care) ends*/
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
