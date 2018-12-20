package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NcqaBCS {

  def main(args: Array[String]): Unit = {


    //<editor-fold desc="Reading Program Arguments and SparkSession Object creation">

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACOL")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to Memory">

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)
    //</editor-fold>

    //<editor-fold desc="Initial join,Allowable Gap,Agefilter and continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spcMeasureTitle)

    /*Loading view table */
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 52 and 74 and female */
    val ageAndGenderFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age52Val, KpiConstants.age74Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).filter($"gender".===("F"))

    /*Continous enrollment calculation*/
    val contEnrollStrtDate = year.toInt - 2 + "-10-01"
    val contEnrollEndDate = year + "-12-31"
    val continiousEnrollDf = ageAndGenderFilterDf.filter(ageAndGenderFilterDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(ageAndGenderFilterDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate)))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*Dinominator Calculation starts*/
    val dinominatorDf = continiousEnrollDf
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    //<editor-fold desc="Dinominator Exclusion step1">

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)

    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*DinominatorExclusion9(Enrolled in an Institutional SNP (I-SNP)) ends*/


    //<editor-fold desc="66 years of age and older with frailty and advanced illness">

    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,fralityValList,primaryDiagCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)


    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,advillValList,primaryDiagCodeSystem)
    val measrForAdvancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*at least 2 Outpatient visit*/
    /*members who has ivd and outpatient*/
    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,outPatValList,outPatCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 non acute inpatient*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    /*union of atleast 2 outpatient visit, Observation visit,Ed visit,Non acute Visit*/
    val unionOfAllAtleastTwoVistDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)

    /*Members who has atleast 2 visits in any of(outpatient visit, Observation visit,Ed visit,Non acute Visit) and advanced ill*/
    val advancedIllAndTwoVistsDf = unionOfAllAtleastTwoVistDf.intersect(measrForAdvancedIllDf)


    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
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

    val age65OrMoreDf = UtilFunctions.ageFilter(ageAndGenderFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*(Members who has age 65 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    val dinoExcl1Df = fralityAndAdvIlDfAndAbove65Df
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion3">

    /*Dinominator Exclusion3(Bilateral mastectomy (Bilateral Mastectomy Value Set) starts)*/
    val bilateralMastectomyValList = List(KpiConstants.bilateralMastectomyVal)
    val icdCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForBilateralMastectomyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,bilateralMastectomyValList,icdCodeSystem).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion3(•	Bilateral mastectomy (Bilateral Mastectomy Value Set) ends)*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion4">

    /*Dinominator Exclusion4(Unilateral mastectomy (Unilateral Mastectomy Value Set) with a bilateral modifier (Bilateral Modifier Value Set) starts)*/
    /*Unilateral mastectomy as primary diagnosis*/
    val unilateralMastectomyValList = List(KpiConstants.unilateralMastectomyVal)
    val joinedForUniMastecAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,unilateralMastectomyValList,icdCodeSystem)

    /*Unilateral mastectomy as proceddure code*/
    val unilateralMastectomyCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForUniMastecAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,unilateralMastectomyValList,unilateralMastectomyCodeSystem)

    /*Members who has a history of Unilateral mastectomy*/
    val unilateralMastectomyDf = joinedForUniMastecAsDiagDf.union(joinedForUniMastecAsProcDf)

    /*bilateral modifier (Bilateral Modifier Value Set)*/
    val bilateralModifierValList = List(KpiConstants.bilateralModifierVal)
    val bilateralModifierCodeSystem = List(KpiConstants.modifierCodeVal)
    val joinedForBilaterlModifierDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,bilateralModifierValList,bilateralModifierCodeSystem).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion4*/
    val unilatAndBilateDf = unilateralMastectomyDf.select(KpiConstants.memberskColName).union(joinedForBilaterlModifierDf)
    /*Dinominator Exclusion4(Unilateral mastectomy (Unilateral Mastectomy Value Set) with a bilateral modifier (Bilateral Modifier Value Set) ends)*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion5">

    /*Dinominator Exclusion5(Two unilateral mastectomies (Unilateral Mastectomy Value Set) with service dates are 14 days or more apart) starts*/
    /*membersks who has atleast 2 unilateral mastectomies*/
    val atLeast2UnilMasMemskDf = unilateralMastectomyDf.groupBy(KpiConstants.memberskColName).agg(count(unilateralMastectomyDf.col(KpiConstants.startDateColName)).alias("count")).filter($"count".>=(2)).select(KpiConstants.memberskColName)

    /*atleast 2 unilateral mastectomies*/
    val atLest2UnilateralmasDf = unilateralMastectomyDf.as("df1").join(atLeast2UnilMasMemskDf.as("df2"),unilateralMastectomyDf.col(KpiConstants.memberskColName) === atLeast2UnilMasMemskDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.member_sk","df1.start_date")
    //atLest2UnilateralmasDf.printSchema()
    val at2Df = atLest2UnilateralmasDf.withColumnRenamed(KpiConstants.startDateColName,"start_date2")
    /*Dinominator Exclusion5(membersks who has atleast 2 unilateral mastectomies and service dates are 14 or more days apart)*/
    val atLest2UnilateralmasAnd14DaysDf = atLest2UnilateralmasDf.as("df1").join(at2Df.as("df2"),$"df1.member_sk" === $"df2.member_sk").filter(datediff($"df1.start_date",$"df2.start_date2").>=(14)).select($"df1.member_sk")
    /*Dinominator Exclusion5(Two unilateral mastectomies (Unilateral Mastectomy Value Set) with service dates are 14 days or more apart) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion6">

    /*Dinominator Exclusion6(History of bilateral mastectomy (History of Bilateral Mastectomy Value Set) starts)*/
    val historyBilateralMastectomyValList = List(KpiConstants.historyBilateralMastectomyVal)
    val joinedForHisBilMastDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,historyBilateralMastectomyValList,icdCodeSystem).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion6(History of bilateral mastectomy (History of Bilateral Mastectomy Value Set) ends)*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion7">

    /*Dinominator Exclusion7(Unilateral Mastectomy and left mastectomy with 14 days service date apart) starts*/
    /*members who has leftModifier*/
    val leftModifierValList = List(KpiConstants.leftModifierVal)
    val leftModifierCodeSystem = List(KpiConstants.modifierCodeVal)
    val joinedForLeftModifierDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,leftModifierValList,leftModifierCodeSystem)

    /*members who has both unilateralMastectomy and LeftModifier on same date*/
    val unilateralAndLeftModDf = unilateralMastectomyDf.as("df1").join(joinedForLeftModifierDf.as("df2"),(unilateralMastectomyDf.col(KpiConstants.memberskColName) === joinedForLeftModifierDf.col(KpiConstants.memberskColName) && unilateralMastectomyDf.col(KpiConstants.startDateColName) === joinedForLeftModifierDf.col(KpiConstants.startDateColName)),KpiConstants.innerJoinType).select("df1.member_sk","df1.start_date")

    /*members who has Left unilateral mastectomy (Unilateral Mastectomy Left Value Set)*/
    val leftUniMasValList = List(KpiConstants.uniMasLeftVal)
    val joinedForLeftUniMasDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,leftUniMasValList,icdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Mmebers who has left mastectomy*/
    val leftMastectomyDf = unilateralAndLeftModDf.union(joinedForLeftUniMasDf)

    /*Members who has both Unilateral Mastectomy and left mastectomy with 14 days service date apart*/
    val unilatAndLeftMasDf = unilateralMastectomyDf.as("df1").join(leftMastectomyDf.as("df2"),unilateralMastectomyDf.col(KpiConstants.memberskColName) === leftMastectomyDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(unilateralMastectomyDf.col(KpiConstants.startDateColName), leftMastectomyDf.col(KpiConstants.startDateColName)).>=(14)).select("df1.member_sk")
    /*Dinominator Exclusion7(Unilateral Mastectomy and left mastectomy with 14 days service date apart) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion8">

    /*Dinominator Exclusion8(unilateral mastectomy and right mastectomy with service dates apart 14 or more) starts*/
    /*Members who has right modifier*/
    val rightModifierValList = List(KpiConstants.rightModifierVal)
    val rightModifierCodeSystem = List(KpiConstants.modifierCodeVal)
    val joinedForRightModifierDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,rightModifierValList,rightModifierCodeSystem)

    /*members who has both unilateralMastectomy and RightModifier on same date*/
    val unilateralAndRightModDf = unilateralMastectomyDf.as("df1").join(joinedForRightModifierDf.as("df2"),(unilateralMastectomyDf.col(KpiConstants.memberskColName) === joinedForRightModifierDf.col(KpiConstants.memberskColName) && unilateralMastectomyDf.col(KpiConstants.startDateColName) === joinedForRightModifierDf.col(KpiConstants.startDateColName)),KpiConstants.innerJoinType).select("df1.member_sk","df1.start_date")

    /*members who has right unilateral mastectomy (Unilateral Mastectomy Left Value Set)*/
    val rightUniMasValList = List(KpiConstants.uniMasRightVal)
    val joinedForRightUniMasDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,rightUniMasValList,icdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Mmebers who has right mastectomy*/
    val rightMastectomyDf = unilateralAndRightModDf.union(joinedForRightUniMasDf)

    /*Members who has both Unilateral Mastectomy and right mastectomy with 14 days service date apart*/
    val unilatAndRightMasDf = unilateralMastectomyDf.as("df1").join(rightMastectomyDf.as("df2"),unilateralMastectomyDf.col(KpiConstants.memberskColName) === rightMastectomyDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(unilateralMastectomyDf.col(KpiConstants.startDateColName), rightMastectomyDf.col(KpiConstants.startDateColName)).>=(14)).select("df1.member_sk")
    /*Dinominator Exclusion8(unilateral mastectomy and right mastectomy with service dates apart 14 or more) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion9">

    /*Dinominator Exclusion9(mastectomy on both the left and right side on the same or different dates of service) starts*/
    /*members who has Absence of the left breast*/
    val absLeftBreastValList = List(KpiConstants.absOfLeftBreastVal)
    val joinedForAbsLeftBreastDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,absLeftBreastValList,icdCodeSystem).select(KpiConstants.memberskColName)

    /*members who has left mastectomy*/
    val leftMastectomyUnionDf = leftMastectomyDf.select(KpiConstants.memberskColName).union(joinedForAbsLeftBreastDf)


    /*members who has Absence of the right breast*/
    val absRightBreastValList = List(KpiConstants.absOfRightBreastVal)
    val joinedForAbsRightBreastDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,absRightBreastValList,icdCodeSystem).select(KpiConstants.memberskColName)

    /*members who has right mastectomy*/
    val rightMastectomyUnionDf = rightMastectomyDf.select(KpiConstants.memberskColName).union(joinedForAbsRightBreastDf)

    /*members who has mastectomy on both the left and right side on the same or different dates of service*/
    val leftAndRightMastectomyDf = leftMastectomyUnionDf.intersect(rightMastectomyUnionDf)
    /*Dinominator Exclusion9(mastectomy on both the left and right side on the same or different dates of service) ends*/
    //</editor-fold>

    /*Dinominator Exclusion*/
    val dinominatorExclDf = dinoExcl1Df.union(joinedForBilateralMastectomyDf).union(unilatAndBilateDf).union(atLest2UnilateralmasAnd14DaysDf).union(joinedForHisBilMastDf).union(unilatAndLeftMasDf).union(unilatAndRightMasDf).union(leftAndRightMastectomyDf)
    val dinominatorAfterExclDf = dinoForKpiCalDf.except(dinominatorExclDf)
    dinominatorExclDf.show()
    //dinominatorAfterExclDf.show()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator calculation starts*/
    val startDate = year.toInt - 2 + "-10-01"
    val currentDate = year + "-12-31"
    val mammographyValList = List(KpiConstants.mammographyVal)
    val joinedForMamoAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,mammographyValList,primaryDiagCodeSystem)
    val measurForMamoAsDiagDf = UtilFunctions.dateBetweenFilter(joinedForMamoAsDiagDf,KpiConstants.startDateColName,startDate,currentDate).select(KpiConstants.memberskColName)

    val mammographyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.bilateralCodeVal,KpiConstants.g204CodeVal,KpiConstants.g206CodeVal)
    val joinedForMamoAsProcDf =  UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.bcsMeasureId,mammographyValList,mammographyCodeSystem)
    val measurForMamoAsProcDf = UtilFunctions.dateBetweenFilter(joinedForMamoAsProcDf,KpiConstants.startDateColName,startDate,currentDate).select(KpiConstants.memberskColName)

    val mammographyDf = measurForMamoAsDiagDf.union(measurForMamoAsProcDf)
    val numeratorDf = mammographyDf.intersect(dinominatorAfterExclDf)
    /*Numerator calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*create the reason valueset for output data*/
    val numeratorValueSet = mammographyValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()

  }
}
