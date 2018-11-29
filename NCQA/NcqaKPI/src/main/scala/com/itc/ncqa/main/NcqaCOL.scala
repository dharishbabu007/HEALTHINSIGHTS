package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, to_date}

import scala.collection.JavaConversions._

object NcqaCOL {

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
    val lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)


    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 51 and 75 and female */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age51Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)





    //<editor-fold desc="Dinominator">

    /*Dinominator Calculation starts*/
    val dinominatorDf = ageFilterDf
    /*Dinominator Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion starts*/

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

    /*(Members who has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDf = fralityDf.union(advancedIllDf)

    val age65OrMoreDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*Dinominator Exclusion2(Members who has age 65 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age65OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*Dinominator Exclusion2 (66 years of age or older with frailty and advanced illness) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion3">

    /*Dinmoinator Exclusion3(•	Colorectal cancer (Colorectal Cancer Value Set) history) starts*/
    /*Colorectal Cancer as primary diagnosis*/
    val colorectalCancerValList = List(KpiConstants.colonoscopyVal)
    val primaryDiagCodeSytem = List(KpiConstants.icdCodeVal)
    val joinedForcolorectalCancerAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMesureId,colorectalCancerValList,primaryDiagCodeSytem).select(KpiConstants.memberskColName)


    /*Colorectal Cancer as proceedure code*/
    val colorectalCancerCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForcolorectalCancerAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,colorectalCancerValList,colorectalCancerCodeSystem).select(KpiConstants.memberskColName)

    val colorectalCancerDf = joinedForcolorectalCancerAsDiagDf.union(joinedForcolorectalCancerAsProcDf)
    /*Dinmoinator Exclusion3(•	Colorectal cancer (Colorectal Cancer Value Set) history) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion4">

    /*Dinominator Exclusion4(Total colectomy (Total Colectomy Value Set) history starts)*/

    /*Total Colectomy as primary diagnosis*/
    val totalColectomyValList = List(KpiConstants.totalColectomyVal)
    val joinedForTotalColectomyAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMesureId,totalColectomyValList,primaryDiagCodeSytem).select(KpiConstants.memberskColName)


    /*Total Colectomy as proceedure code*/
    val totalColectomyCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTotalColectomyValAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,totalColectomyValList,totalColectomyCodeSystem).select(KpiConstants.memberskColName)

    val totalColectomyValDf = joinedForTotalColectomyAsDiagDf.union(joinedForTotalColectomyValAsProcDf)
    /*Dinominator Exclusion4(•	Total colectomy (Total Colectomy Value Set) history ends)*/
    //</editor-fold>


    /*Dinominator Exclusion ends*/
    //</editor-fold>

    //<editor-fold desc="Numearator calculation">

    /*Numerator starts*/

    //<editor-fold desc="Numerator1">

    /*Numerator1(•	Fecal occult blood test (FOBT Value Set) starts)*/
    val fobtValList = List(KpiConstants.fobtVal)
    val fobtCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal)
    val joinedForFobtDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,fobtValList,fobtCodeSystem)
    val measurementForFobDf = UtilFunctions.mesurementYearFilter(joinedForFobtDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    /*Numerator1(•	Fecal occult blood test (FOBT Value Set) ends)*/
    //</editor-fold>

    //<editor-fold desc="Numerator2">

    /*Numerator2(•	Flexible sigmoidoscopy (Flexible Sigmoidoscopy Value Set) during year and 4 years prior to the year) starts*/
    /*Flexible sigmoidoscopy as primary diagnosis*/
    val flexibleSigmdoscopyValList = List(KpiConstants.flexibleSigmodoscopyVal)
    //val primaryDiagCodeSytem = List(KpiConstants.icdCodeVal)
    val joinedForFlexibleSigmoAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMesureId,flexibleSigmdoscopyValList,primaryDiagCodeSytem)
    val measurementForFlxSigmoAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForFlexibleSigmoAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementFourYearUpper).select(KpiConstants.memberskColName)


    /*Flexible sigmoidoscopy as proceedure code*/
    val flexibleSigmCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForFlexibleSigmoAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,flexibleSigmdoscopyValList,flexibleSigmCodeSystem)
    val measurementForFlxSigmoAsProcDf = UtilFunctions.mesurementYearFilter(joinedForFlexibleSigmoAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementFourYearUpper).select(KpiConstants.memberskColName)

    val flexiblesigmidoscopyDf = measurementForFlxSigmoAsDiagDf.union(measurementForFlxSigmoAsProcDf)
    /*Numerator2(•	Flexible sigmoidoscopy (Flexible Sigmoidoscopy Value Set) during year and 4 years prior to the year) ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator3">

    /*Numerator3(•	Colonoscopy (Colonoscopy Value Set) during the measurement year or the nine years prior to the measurement year) starts*/
    /*Colonoscopy as primary diagnosis*/
    val colonoscopyValList = List(KpiConstants.colonoscopyVal)
    val joinedForColonoscopyAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.colMesureId,colonoscopyValList,primaryDiagCodeSytem)
    val measurementForColonoscopyAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForColonoscopyAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementNineYearUpper).select(KpiConstants.memberskColName)


    /*Colonoscopy as proceedure code*/
    val colonoscopyCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForColonoscopyAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,colonoscopyValList,colonoscopyCodeSystem)
    val measurementForColonoscopyAsProcDf = UtilFunctions.mesurementYearFilter(joinedForColonoscopyAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementNineYearUpper).select(KpiConstants.memberskColName)

    val colonoscopyDf = measurementForColonoscopyAsDiagDf.union(measurementForColonoscopyAsProcDf)
    /*Numerator3(•	Colonoscopy (Colonoscopy Value Set) during the measurement year or the nine years prior to the measurement year) ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator4">

    /*Numerator4(•	CT colonography (CT Colonography Value Set) during the measurement year or the four years prior to the measurement year starts)*/
    val ctColonographyValList = List(KpiConstants.ctColonographyVal)
    val ctColonographyCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForctColonographyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,ctColonographyValList,ctColonographyCodeSystem)
    val measurementForctColonographyDf = UtilFunctions.mesurementYearFilter(joinedForctColonographyDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementFourYearUpper).select(KpiConstants.memberskColName)
    /*Numerator4(•	CT colonography (CT Colonography Value Set) during the measurement year or the four years prior to the measurement year ends)*/
    //</editor-fold>

    //<editor-fold desc="Numerator5">

    /*Numerator5(•	FIT-DNA test (FIT-DNA Value Set) during the measurement year or the two years prior to the measurement year) starts*/
    val fitDnaValList = List(KpiConstants.fitDnaVal)
    val fitDnaCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal)
    val joinedForfitDnaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.colMesureId,fitDnaValList,fitDnaCodeSystem)
    val measurementForfitDnaDf = UtilFunctions.mesurementYearFilter(joinedForfitDnaDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementThreeYearUpper).select(KpiConstants.memberskColName)
    /*Numerator5(•	FIT-DNA test (FIT-DNA Value Set) during the measurement year or the two years prior to the measurement year) ends*/
    //</editor-fold>

    /*Numerator(One or more screenings for colorectal cancer)*/
    val numeratorUnionDf = measurementForFobDf.union(flexiblesigmidoscopyDf).union(colonoscopyDf).union(measurementForctColonographyDf).union(measurementForfitDnaDf)
    /*Numerator ends*/
    //</editor-fold>


  }
}
