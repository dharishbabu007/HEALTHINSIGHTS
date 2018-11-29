package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NcqaBCS {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACOL")
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

    /*Dinominator Calculation starts*/
    val dinominatorDf = continiousEnrollDf
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation ends*/


    /*Dinominator Exclusion calculation starts*/

    /*Dinominator Exclusion1 starts()*/
    /*Dinominator Exclusion1 ends()*/


    //<editor-fold desc="Dinominator Exclusion2">

    /*Dinominator Exclusion2 starts(66 years or more age and frailty (Frailty Value Set) and advanced illness during the measurement year)*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)

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
    val joinedForTwoOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcOutPatientValueSet,KpiConstants.spcOutPatientCodeSystem)
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
    /*members who has atleast one acute inpatient*/
    val joinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spcMeasureId,KpiConstants.spcAcuteInpatientValueSet,KpiConstants.spcAcuteInpatientCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
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

    val age65OrMoreDf = UtilFunctions.ageFilter(ageAndGenderFilterDf, KpiConstants.dobColName, year, KpiConstants.age66Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*Dinominator step3(Members who has age 65 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*Dinominator Exclusion2 (66 years of age or older with frailty and advanced illness) ends*/
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
    val dinominatorExclDf = fralityAndAdvIlDfAndAbove65Df.union(joinedForBilateralMastectomyDf).union(unilatAndBilateDf).union(atLest2UnilateralmasAnd14DaysDf).union(joinedForHisBilMastDf).union(unilatAndLeftMasDf).union(unilatAndRightMasDf).union(leftAndRightMastectomyDf)
    val dinominatorAfterExclDf = dinoForKpiCalDf.except(dinominatorExclDf)
    dinominatorExclDf.show()
    //dinominatorAfterExclDf.show()
    /*Dinominator Exclusion calculation ends*/


    spark.sparkContext.stop()

  }
}
