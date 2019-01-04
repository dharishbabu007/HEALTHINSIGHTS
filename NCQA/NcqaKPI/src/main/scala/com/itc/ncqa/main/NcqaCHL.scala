
package com.itc.ncqa.main
import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import scala.collection.JavaConversions._

object NcqaCHL {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading Program Arguments and Saprksession Object creation">

    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    val measureId = args(4)
    var data_source =""
    /*define data_source based on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }


    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)


    /*define data_source based on program type. */
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACHL1")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading the Required Table to memory">

    import spark.implicits._
    var lookupTableDf = spark.emptyDataFrame

    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val factRxClaimsDF = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants. factRxClaimTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Continous Enrollment and Allowable Gap filter Calculations">

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,refLobDf,dimFacilityDf,lob_name,KpiConstants.chlMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))
    var lookUpDf = spark.emptyDataFrame
    /*Loading allowable Gap view based on the Lob Name*/
    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }

    /*common filter checking*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    //</editor-fold>

    //<editor-fold desc="Age and Gender Filter">

    /*Age filter*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf,KpiConstants.dobColName,year,KpiConstants.age16Val,KpiConstants.age24Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    /*Gender filter*/
    val genderFilterDf =ageFilterDf.filter($"gender".===("F"))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*Dinominator Calculation Starts*/
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    val icdCodeSystem = List(KpiConstants.icdCodeVal)

    /*Sexual Activity,Pregnancy dinominator Calculation during the last 2 year.*/
    val chlPregAndSexualValList = List(KpiConstants.pregnancyVal,KpiConstants.sexualActivityVal)
    val hedisJoinedForFirstDino = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.chlMeasureId,chlPregAndSexualValList,icdCodeSystem)
    val sexualAndPregnancyDistinctDf = UtilFunctions.measurementYearFilter(hedisJoinedForFirstDino,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select("member_sk").distinct()


    /*Pregnancy Tests Dinominator Calculation during the last 2 year.*/
    val chlPregnTestValList = List(KpiConstants.pregnancyTestVal)
    val chlPregnTestCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForSecondDino = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType, KpiConstants.chlMeasureId,chlPregnTestValList,chlPregnTestCodeSystem)
    val pregnancyTestMeasurementDf = UtilFunctions.measurementYearFilter(hedisJoinedForSecondDino,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val pregnancyTestDistinctDf = pregnancyTestMeasurementDf.select("member_sk").distinct()


    /*Pharmacy data Dinominator(Contraceptive Medication during last 2 year.)*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
    val contraceptiveMedValList = List(KpiConstants.contraceptiveMedicationVal)
    val joinedForContraceptiveDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDF,ref_medvaluesetDf,KpiConstants.chlMeasureId,contraceptiveMedValList)
    val pharmacyDistinctDf = UtilFunctions.measurementYearFilter(joinedForContraceptiveDf,KpiConstants.rxStartDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select("member_sk").distinct()

    /*union of three different dinominator calculation*/
    val unionOfThreeDinominatorDf = sexualAndPregnancyDistinctDf.union(pregnancyTestDistinctDf).union(pharmacyDistinctDf)
    val dinominatorDf = genderFilterDf.as("df1").join(unionOfThreeDinominatorDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.*")
    val dinominatorForKpiCal = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    dinominatorForKpiCal.show()
    /*End of dinominator Calculation */
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion">

    //<editor-fold desc="Dinominator exclusion1">

    /*Dinominator Exclusion1(Hospice Members) starta*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion1(Hospice Members) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclsuion2">

    //<editor-fold desc="Dinominator Exclusion2 sub1">

    /* Dinominator Exclusion2 sub1(pregnancy test Exclusion during measurement period and a Retinoid Medications) starts*/
    val pregExclTestValList = List(KpiConstants.pregnancyExcltestVal)
    val pregExclTestCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForPregnancyExclusion = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.chlMeasureId,pregExclTestValList,pregExclTestCodeSystem)
    val measurementDfPregnancyExclusion = UtilFunctions.measurementYearFilter(hedisJoinedForPregnancyExclusion,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select("member_sk").distinct()

    /*Retonoid Medications during pregnancy or six days before pregnancy test*/
    val retronoidMedValList = List(KpiConstants.retinoidMedicationval)
    val joinedForRetronoidMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDF,ref_medvaluesetDf,KpiConstants.chlMeasureId,retronoidMedValList)
    val joinedForPreTestAndRetroDf = pregnancyTestMeasurementDf.as("df1").join(joinedForRetronoidMedDf.as("df2"),pregnancyTestMeasurementDf.col(KpiConstants.memberskColName) === joinedForRetronoidMedDf.col(KpiConstants.memberskColName) , KpiConstants.innerJoinType).select("df1.member_sk","df1.start_date","df2.rx_start_date")

    /*members who has done retonoid on or after six days of pregnancy test*/
    val dayFilteredDfForIsoExcl = joinedForPreTestAndRetroDf.filter((datediff(joinedForPreTestAndRetroDf.col(KpiConstants.rxStartDateColName), joinedForPreTestAndRetroDf.col(KpiConstants.startDateColName)).===(0)) ||  ( datediff(joinedForPreTestAndRetroDf.col(KpiConstants.rxStartDateColName), joinedForPreTestAndRetroDf.col(KpiConstants.startDateColName)).===(6)))
    /*pregnancyExclusion and isotretinoinDf join*/
    val prgnancyExclusionandIsoDf = measurementDfPregnancyExclusion.as("df1").join(dayFilteredDfForIsoExcl.as("df2"),measurementDfPregnancyExclusion.col(KpiConstants.memberskColName) === dayFilteredDfForIsoExcl.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select(measurementDfPregnancyExclusion.col(KpiConstants.memberskColName)).distinct()
    /* Dinominator Exclusion2 sub1(pregnancy test Exclusion during measurement period and a Retinoid Medications) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclsuion2 sub2">

    /*Dinominator Exclusion2 sub2(pregnancy test Exclusion during measurement period and a Diagnostic Radiology) starts*/
    /*Diagnostic Radiology Value Set Exclusion*/
    val diagRadValList = List(KpiConstants.diagnosticRadVal)
    val diagRadCodeSystem = List(KpiConstants.cptCodeVal)
    val hedisJoinedForDigRadExclusion = pregnancyTestMeasurementDf.as("df1").join(factClaimDf.as("df2"), pregnancyTestMeasurementDf.col(KpiConstants.memberskColName) === factClaimDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code",KpiConstants.innerJoinType).filter($"measureid".===(KpiConstants.chlMeasureId).&&($"valueset".isin(diagRadValList:_*)).&&($"codesystem".isin(diagRadCodeSystem:_*))).select("df1.*","df2.start_date_sk")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val startDateValAddedDfDigRadExclusion = hedisJoinedForDigRadExclusion.as("df1").join(dimDateDf.as("df2"), hedisJoinedForDigRadExclusion.col(KpiConstants.startDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName)).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.startTempColName).drop(KpiConstants.startDateSkColName)
    val dateTypeDfDigRadExclusion = startDateValAddedDfDigRadExclusion.withColumn(KpiConstants.diagStartColName, to_date(startDateValAddedDfDigRadExclusion.col(KpiConstants.startTempColName), "dd-MMM-yyyy")).drop( startDateValAddedDfDigRadExclusion.col(KpiConstants.startTempColName))
    val dayFilterdDigRadExclusion = dateTypeDfDigRadExclusion.filter(datediff(dateTypeDfDigRadExclusion.col(KpiConstants.diagStartColName),dateTypeDfDigRadExclusion.col(KpiConstants.startDateColName)).===(0) ||(datediff(dateTypeDfDigRadExclusion.col(KpiConstants.diagStartColName),dateTypeDfDigRadExclusion.col(KpiConstants.startDateColName)).===(6))).select(KpiConstants.memberskColName).distinct()

    /*pregnancyExclusion and Diagnostic Radiology join*/
    val pregnancyExclusionandDigRadDf = measurementDfPregnancyExclusion.as("df1").join(dayFilterdDigRadExclusion.as("df2"),measurementDfPregnancyExclusion.col(KpiConstants.memberskColName) === dayFilterdDigRadExclusion.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select(measurementDfPregnancyExclusion.col(KpiConstants.memberskColName)).distinct()
    /*Dinominator Exclusion2 sub2(pregnancy test Exclusion during measurement period and a Diagnostic Radiology) ends*/
    //</editor-fold>

    /*prgnancyExclusionandIsoDf and pregnancyExclusionandDigRadDf union */
    val unionOfExclusionDf = prgnancyExclusionandIsoDf.union(pregnancyExclusionandDigRadDf).distinct()

    /*members who are only present in Pregnancy test and not in other category in Dinominator*/
    val pregnancyTestOnlyDf = pregnancyTestDistinctDf.except(sexualAndPregnancyDistinctDf.union(pharmacyDistinctDf))
    /*Members who has pregnancy test and Retonoid or Dignostic Radiology*/
    val secondDinominatorExclusion = pregnancyTestOnlyDf.intersect(unionOfExclusionDf)
    //</editor-fold>

    /*union of hospice and the secondDinominatorExclusion*/
    val dinominatorExclusionDf = hospiceDf.union(secondDinominatorExclusion).distinct()
    /*End of Dinominator Exclusion Starts*/
    /*Final dinominator after Exclusion*/
    val dinominatorFinalDf = dinominatorForKpiCal.except(dinominatorExclusionDf)
    dinominatorFinalDf.show()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator calculation(Chlamydia Tests Value Set) during measurement year starts*/
    val chlmydiaTestValList = List(KpiConstants.chalamdiaVal)
    val chlmydiaTestCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal)
    val hedisJoinedChlmydiaTestDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.chlMeasureId,chlmydiaTestValList,chlmydiaTestCodeSystem)
    val measurForChlmydiaDf = UtilFunctions.measurementYearFilter(hedisJoinedChlmydiaTestDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val).select("member_sk")

    val numeratorDistinctDf = measurForChlmydiaDf.intersect(dinominatorFinalDf)
    numeratorDistinctDf.show()
    /*Numerator calculation(Chlamydia Tests Value Set) during measurement year ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = chlmydiaTestValList
    val dinominatorExclValueSet = pregExclTestValList:::diagRadValList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.chlMeasureId)
    val numExclDf = spark.emptyDataFrame

    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, numeratorDistinctDf, numExclDf, listForOutput, sourceAndMsrIdList)
   // outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto("ncqa_sample.gaps_in_hedis_test")
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }

}


















