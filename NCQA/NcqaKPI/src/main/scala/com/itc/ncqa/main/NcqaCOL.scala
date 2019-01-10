package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, to_date}

import scala.collection.JavaConversions._

object NcqaCOL {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and spark session Object creation">

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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAURI")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Continuous Enrollment, Allowable Gap and Age filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spcMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year.toInt -1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Loading view table */
    val lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /* Age filter (filter out the members whoose age is between 51 and 75)*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age51Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = ageFilterDf
    val dinoForKpiCalDf =  dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion starts*/

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)

    //<editor-fold desc="Dinominator Exclsuion1">

    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion2">

    /*Step5 Exclusion(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*Step5 Exclusion(Enrolled in an Institutional SNP (I-SNP)) ends*/


    //<editor-fold desc="(66 - 80 years of age and older with frailty and advanced illness)">

    /*(66 - 80 years of age and older with frailty and advanced illness) starts*/


    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,fralityValList,primaryDiagCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)

    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,advillValList,primaryDiagCodeSystem)
    val measrForAdvancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*at least 2 Outpatient visit*/
    val outPatientValueSet = List(KpiConstants.outPatientVal)
    val outPatientCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,outPatientValueSet,outPatientCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    val acuteInPatwoTeleDf = twoNonAcutePatDf

    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

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

    val age66OrMoreDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age67Val, KpiConstants.age80Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*(Members who has age 66 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age66OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    val dinoExclusion2Df = fralityAndAdvIlDfAndAbove65Df
    //</editor-fold>

    //<editor-fold desc="Dinmoinator Exclusion3(•	Colorectal cancer (Colorectal Cancer Value Set) history)">

    /*Colorectal Cancer as primary diagnosis*/
    val colorectalCancerValList = List(KpiConstants.colonoscopyVal)
    val primaryDiagCodeSytem = List(KpiConstants.icdCodeVal)
    val joinedForcolorectalCancerAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,colorectalCancerValList,primaryDiagCodeSytem)
                                                         .select(KpiConstants.memberskColName)


    /*Colorectal Cancer as proceedure code*/
    val colorectalCancerCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForcolorectalCancerAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,colorectalCancerValList,colorectalCancerCodeSystem)
                                                         .select(KpiConstants.memberskColName)

    val colorectalCancerDf = joinedForcolorectalCancerAsDiagDf.union(joinedForcolorectalCancerAsProcDf)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion4(Total colectomy (Total Colectomy Value Set) history">

    /*Total Colectomy as primary diagnosis*/
    val totalColectomyValList = List(KpiConstants.totalColectomyVal)
    val joinedForTotalColectomyAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,totalColectomyValList,primaryDiagCodeSytem)
                                                       .select(KpiConstants.memberskColName)


    /*Total Colectomy as proceedure code*/
    val totalColectomyCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTotalColectomyValAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,totalColectomyValList,totalColectomyCodeSystem)
                                                          .select(KpiConstants.memberskColName)

    val totalColectomyValDf = joinedForTotalColectomyAsDiagDf.union(joinedForTotalColectomyValAsProcDf)

    //</editor-fold>

    val dinominatorExclDf = hospiceDf.union(dinoExclusion2Df).union(colorectalCancerDf).union(totalColectomyValDf)
    val dinominatorAfterExclDf = dinoForKpiCalDf.except(dinominatorExclDf)
    /*Dinominator Exclusion ends*/
    //</editor-fold>

    //<editor-fold desc="Numearator calculation">

    /*Numerator starts*/

    //<editor-fold desc="Numerator1(Fecal occult blood test (FOBT Value Set)">

    val fobtValList = List(KpiConstants.fobtVal)
    val fobtCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal)
    val joinedForFobtDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,fobtValList,fobtCodeSystem)
    val measurementForFobDf = UtilFunctions.measurementYearFilter(joinedForFobtDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                           .select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Numerator2">

    /*Numerator2(•	Flexible sigmoidoscopy (Flexible Sigmoidoscopy Value Set) during year and 4 years prior to the year) starts*/
    /*Flexible sigmoidoscopy as primary diagnosis*/
    val flexibleSigmdoscopyValList = List(KpiConstants.flexibleSigmodoscopyVal)
    val joinedForFlexibleSigmoAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,flexibleSigmdoscopyValList,primaryDiagCodeSytem)
    val measurementForFlxSigmoAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFlexibleSigmoAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement5Val)
                                                      .select(KpiConstants.memberskColName)


    /*Flexible sigmoidoscopy as proceedure code*/
    val flexibleSigmCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForFlexibleSigmoAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,flexibleSigmdoscopyValList,flexibleSigmCodeSystem)
    val measurementForFlxSigmoAsProcDf = UtilFunctions.measurementYearFilter(joinedForFlexibleSigmoAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement5Val)
                                                      .select(KpiConstants.memberskColName)

    val flexiblesigmidoscopyDf = measurementForFlxSigmoAsDiagDf.union(measurementForFlxSigmoAsProcDf)
    //</editor-fold>

    //<editor-fold desc="Numerator3">

    /*Numerator3(•	Colonoscopy (Colonoscopy Value Set) during the measurement year or the nine years prior to the measurement year) starts*/
    /*Colonoscopy as primary diagnosis*/
    val colonoscopyValList = List(KpiConstants.colonoscopyVal)
    val joinedForColonoscopyAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMeasureId,colonoscopyValList,primaryDiagCodeSytem)
    val measurementForColonoscopyAsDiagDf = UtilFunctions.measurementYearFilter(joinedForColonoscopyAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement10Val)
                                                         .select(KpiConstants.memberskColName)


    /*Colonoscopy as proceedure code*/
    val colonoscopyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForColonoscopyAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,colonoscopyValList,colonoscopyCodeSystem)
    val measurementForColonoscopyAsProcDf = UtilFunctions.measurementYearFilter(joinedForColonoscopyAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement10Val)
                                                         .select(KpiConstants.memberskColName)

    val colonoscopyDf = measurementForColonoscopyAsDiagDf.union(measurementForColonoscopyAsProcDf)
    //</editor-fold>

    //<editor-fold desc="Numerator4">

    /*Numerator4(•	CT colonography (CT Colonography Value Set) during the measurement year or the four years prior to the measurement year starts)*/
    val ctColonographyValList = List(KpiConstants.ctColonographyVal)
    val ctColonographyCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForctColonographyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,ctColonographyValList,ctColonographyCodeSystem)
    val measurementForctColonographyDf = UtilFunctions.measurementYearFilter(joinedForctColonographyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement5Val)
                                                      .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Numerator5">

    /*Numerator5(•	FIT-DNA test (FIT-DNA Value Set) during the measurement year or the two years prior to the measurement year) starts*/
    val fitDnaValList = List(KpiConstants.fitDnaVal)
    val fitDnaCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal)
    val joinedForfitDnaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMeasureId,fitDnaValList,fitDnaCodeSystem)
    val measurementForfitDnaDf = UtilFunctions.measurementYearFilter(joinedForfitDnaDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement3Val)
                                              .select(KpiConstants.memberskColName)
    /*Numerator5(•	FIT-DNA test (FIT-DNA Value Set) during the measurement year or the two years prior to the measurement year) ends*/
    //</editor-fold>

    /*Numerator(One or more screenings for colorectal cancer)*/
    val numeratorUnionDf = measurementForFobDf.union(flexiblesigmidoscopyDf).union(colonoscopyDf).union(measurementForctColonographyDf).union(measurementForfitDnaDf)
    val numeratorDf = numeratorUnionDf.intersect(dinominatorAfterExclDf)
    /*Numerator ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*create the reason valueset for output data*/
    val numeratorValueSet = fobtValList ::: flexibleSigmdoscopyValList ::: colonoscopyValList
    val dinominatorExclValueSet = fralityValList ::: colorectalCancerValList
    val numeratorExclValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
