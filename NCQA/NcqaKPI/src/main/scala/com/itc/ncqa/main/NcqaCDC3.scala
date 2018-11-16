package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import scala.collection.JavaConversions._

object NcqaCDC3 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC3")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
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


    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)


    /*Join dimMember,factclaim,factmembership,reflob,dimfacility,dimlocation.*/
    val joinedForInitialFilterDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cdcMeasureTitle)


    /*Load the look up view based on the lob_name*/
    var lookUpTableDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpTableDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }


    /*Common filter (Removing the elements who has a gap of 45 days or 60 days)*/
    val commonFilterDf = joinedForInitialFilterDf.as("df1").join(lookUpTableDf.as("df2"), joinedForInitialFilterDf.col(KpiConstants.memberskColName) === lookUpTableDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter("start_date is null").select("df1.*")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //ageFilterDf.orderBy("member_sk").show(50)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)


    /*calculating Dinominator*/

    /*Dinominator First condition */
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdcDiabetesvalueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForFirstDino = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino, "start_date", year, 0, 730).select("member_sk").distinct()


    /*Dinominator Second Condition*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"), $"df2.ndc_number" === $"df3.ndc_code", "inner").filter($"measure_id".===(KpiConstants.cdcMeasureId)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()


    /*union of first and second dinominator condition*/
    val dinominatorUnionDf = measurementForFirstDino.union(MeasurementForSecondDinoDf)

    /*dinominator  for ouput Calculation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === dinominatorUnionDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.*")
    val dinoMemberSkDf = dinominatorDf.select(KpiConstants.memberskColName)
    //dinominatorDf.show(50)


    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)


    /*dinominator Exclusion 2*/
    val hedisJoinedForDiabetesExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdcDiabetesExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDiabetesExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName).distinct()


    /*dinominator Exclusion 3 (Age filter  Exclusion)*/
    val dinominatorAgeFilterExclDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age65Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).select(KpiConstants.memberskColName).distinct()

    /*dinominator Exclusion 4 (CABG Exclusion)*/
    val hedisJoinedForCabgValueExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3CabgValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementCabgValueExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForCabgValueExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName).distinct()

    /*dinominator Exclusion 5 (PCI exclusion)*/
    val hedisJoinedForPciValueExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3PciValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementPciValueExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPciValueExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName).distinct()


    /*dinominator Exclusion 6 (IVD condition)*/
    val hedisJoinedForOutPatExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3OutPatientValueSet, KpiConstants.cdc3OutPatientCodeSystem)
    val measurementOutPatExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForOutPatExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    /*IVD EXCL*/
    val hedisJoinedForIvdExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3IvdExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementIvdExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForIvdExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)

    /*join measurementOutPatExclDf and measurementIvdExclDf for getting the member_sk that are present in both the case*/
    val joinOutPatAndIvdDf = measurementOutPatExclDf.as("df1").join(measurementIvdExclDf.as("df2"), measurementOutPatExclDf.col(KpiConstants.memberskColName) === measurementIvdExclDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk").distinct()


    //Acute Inpatient
    val hedisJoinedForAccuteInPatExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3AccuteInPtValueSet, KpiConstants.cdc3AccuteInPtCodeSystem)
    val measurementAccuteInPatExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForAccuteInPatExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)

    /*Join Acute Inpatient with Ivd Excl*/
    val joinAccuteInPatAndIvdDf = measurementAccuteInPatExclDf.as("df1").join(measurementIvdExclDf.as("df2"), measurementAccuteInPatExclDf.col(KpiConstants.memberskColName) === measurementIvdExclDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk").distinct()

    /*union of joinOutPatAndIvdDf and joinAccuteInPatAndIvdDf*/
    val ivdExclDf = joinOutPatAndIvdDf.union(joinAccuteInPatAndIvdDf)

    /*dinominator Exclusion 6 (IVD condition) Ends */


    /*dinominator Exclusion 7 (Thoracic aortic aneurysm)*/
    val hedisJoinedForThAoAnExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3ThAoAnvalueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementThAoAnExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForThAoAnExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)

    /*join measurementOutPatExclDf with measurementThAoAnExclDf for the first condition*/
    val joinedOutPatAndThAoAnExclDf = measurementOutPatExclDf.as("df1").join(measurementThAoAnExclDf.as("df2"), measurementOutPatExclDf.col(KpiConstants.memberskColName) === measurementThAoAnExclDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk").distinct()

    /*join measurementAccuteInPatExclDf with measurementThAoAnExclDf for the second condition*/
    val joinedAccuteInAndThAoAnExclDf = measurementAccuteInPatExclDf.as("df1").join(measurementThAoAnExclDf.as("df2"), measurementAccuteInPatExclDf.col(KpiConstants.memberskColName) === measurementThAoAnExclDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk").distinct()

    /*union of joinedOutPatAndThAoAnExclDf and joinedAccuteInAndThAoAnExclDf*/
    val thAoAnExclDf = joinedOutPatAndThAoAnExclDf.union(joinedAccuteInAndThAoAnExclDf)

    /*dinominator Exclusion 7 (Thoracic aortic aneurysm) ends*/


    /*Dinominator Exclusion 8 Starts*/

    /*Chronic Heart Failure Exclusion*/
    val hedisJoinedForChfExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3ChfExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementChfExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForChfExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*MI*/
    val hedisJoinedForMiExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3MiExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementMiExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMiExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*ESRD AS Primary Diagnosis */
    val hedisJoinedForEsrdAsDiagExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3MiExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementEsrdAsDiagExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMiExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*ESRD and  ESRD Obsolete as proceedure code*/
    val hedisJoinedForEsrdAsProExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3EsrdExclValueSet, KpiConstants.cdc3EsrdExclcodeSystem)
    val measurementEsrdAsProExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsProExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*ESRD Exclusion by union ESRD AS Primary Diagnosis and ESRD and  ESRD Obsolete as proceedure code*/
    val esrdExclDf = measurementEsrdAsDiagExclDf.union(measurementEsrdAsProExclDf)

    /*CKD Stage 4  Exclusion*/
    val hedisJoinedForckdExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3CkdStage4ValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementckdExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForckdExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Dementia Exclusion*/
    val hedisJoinedForDementiaExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3DementiaExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDementiaExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDementiaExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Blindness Exclusion*/
    val hedisJoinedForBlindnessExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3BlindnessExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementBlindnessExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBlindnessExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()


    /* Lower extremity amputation Exclusion as primary diagnosis*/
    val hedisJoinedForLeaAsDiagExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3LEAExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementLeaAsDiagExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLeaAsDiagExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Lower extremity amputation Exclusion as Proceedure Code*/
    val hedisJoinedForLeaAsProExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc3LEAExclValueSet, KpiConstants.cdc3LEAExclCodeSystem)
    val measurementLeaAsProExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLeaAsProExclDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*union of measurementLeaAsDiagExclDf and measurementLeaAsProExclDf for Lower extremity amputation condition*/
    val leaExclDf = measurementLeaAsDiagExclDf.union(measurementLeaAsProExclDf).distinct()


    /*union of all Dinominator8 Exclusions*/
    val unionOfAllDinominator8ExclDf = measurementChfExclDf.union(measurementMiExclDf).union(esrdExclDf).union(measurementDementiaExclDf).union(measurementBlindnessExclDf).union(leaExclDf).union(measurementckdExclDf)

    /*Union of all 8 Dinominator Exclusion Condition*/
    val unionOfAllDinominatorExclDf = hospiceDf.union(measurementDiabetesExclDf).union(dinominatorAgeFilterExclDf).union(measurementCabgValueExclDf).union(measurementPciValueExclDf).union(ivdExclDf).union(thAoAnExclDf).union(unionOfAllDinominator8ExclDf).distinct()
    //unionOfAllDinominatorExclDf.show(50)


    /*CDC3 Dinominator for kpi calculation */
    val cdc3DinominatorForKpiDf = dinoMemberSkDf.except(unionOfAllDinominatorExclDf)

    /*Numerator Calculation*/


    /*Numerator 1 (who has done Hemoglobin A1c (HbA1c) testing)*/
    val hedisJoinedForHba1cDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdcNumerator1ValueSet, KpiConstants.cdc1NumeratorCodeSystem)
    val measurementForHba1cDf = UtilFunctions.mesurementYearFilter(hedisJoinedForHba1cDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName).distinct()

    /*Numerator2 (HbA1c Level Greater Than 9.0)*/
    val hedisJoinedForNumeratorDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cdcMeasureId, KpiConstants.cdc2Numerator2ValueSet, KpiConstants.cdc2NumeratorCodeSystem)
    val measurementForNumerator = UtilFunctions.mesurementYearFilter(hedisJoinedForNumeratorDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    val numeratorDf = measurementForNumerator.intersect(measurementForHba1cDf)

    val cdc3NumeratorDf = numeratorDf.intersect(cdc3DinominatorForKpiDf)



    //print("counts:"+dinoMemberSkDf.count()+","+unionOfAllDinominatorExclDf.count()+","+cdc3NumeratorDf)

    /*common output creation for facts_gaps_in_hedis table*/
    val numericReasonValueSet = KpiConstants.cdcNumerator1ValueSet ::: KpiConstants.cdc2Numerator2ValueSet
    val dinoExclReasonValueSet = KpiConstants.cdcDiabetesExclValueSet ::: KpiConstants.cdc3CabgValueSet ::: KpiConstants.cdc3PciValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numericReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cdc3MeasureId)

    /*creating empty dataframe for numEWxclDf*/
    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, unionOfAllDinominatorExclDf, cdc3NumeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factGapsInHedisTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.cdcMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+KpiConstants.factHedisQmsTblName)
    spark.sparkContext.stop()

  }

}
