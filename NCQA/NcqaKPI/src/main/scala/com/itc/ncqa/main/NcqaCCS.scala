package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.commons.math3.analysis.function
import org.apache.commons.math3.analysis.function.Abs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import org.apache.commons.math3.analysis.function

object NcqaCCS {

  def main(args: Array[String]): Unit = {


    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source = ""
    var measurementYearUpper = 0

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACCS")
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
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.CcsMeasureTitle)

    /*Loading view table */
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*define Lob_Name  is commercial three years as mesurement year else one year*/
    if("Medicaid".equals(lob_name)) {
      measurementYearUpper = KpiConstants.measurementOneyearUpper
    }
    else {
      measurementYearUpper = KpiConstants.measuremetTwoYearUpper
    }

    /*Remove the Elements who are present on the view table.*/

    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*Continous enrollment calculation*/
    val contEnrollStrtDate =KpiConstants.measurementYearLower + "-01-01"
    val contEnrollEndDate = measurementYearUpper + "-12-31"
    val continiousEnrollDf = commonFilterDf.filter(commonFilterDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(commonFilterDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate)))


    /*filter out the members whoose age is between 24 and 64 and female */
    val ageAndGenderFilter24To64Df = UtilFunctions.ageFilter(continiousEnrollDf, KpiConstants.dobColName, year, KpiConstants.age24Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).filter($"gender".===("F"))

    val ageAndGenderFilter30To64Df = UtilFunctions.ageFilter(continiousEnrollDf, KpiConstants.dobColName, year, KpiConstants.age24Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).filter($"gender".===("F"))



    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)


    /*Dinominator Calculation starts*/


    val dinominatorDf = continiousEnrollDf  /*d*/

    /*Dinominator Exclusion Calculation as proceedure code*/

    val hedisJoinedForDinoCcsAsPrDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, KpiConstants.ccsDinomenatorExclValueSet, KpiConstants.ccsDinomenatorExclCodeSystem)
    val measurementForDinoCcsAsPrDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDinoCcsAsPrDf, "start_date", year, KpiConstants.measurementYearLower, measurementYearUpper).select(KpiConstants.memberskColName).distinct()

    /*Dinominator Exclusion Calculation as primary diagnosis*/



    val hedisJoinedForDinoCcsAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, KpiConstants.ccsDinomenatorExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForDinoCcsAsDiagDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDinoCcsAsDiagDf, "start_date", year, KpiConstants.measurementYearLower, measurementYearUpper).select(KpiConstants.memberskColName).distinct()

    val finalDinoExclusion1Df = measurementForDinoCcsAsPrDf.union(measurementForDinoCcsAsDiagDf)

    /*Dinominator Exclusion2 (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val measurementExcl = UtilFunctions.mesurementYearFilter(continiousEnrollDf, "start_date", year,  KpiConstants.measurementYearLower, measurementYearUpper).select(KpiConstants.memberskColName)
    val unionOfDinoExclAndYearDf = measurementExcl.union(hospiceDinoExclDf)
    val dinominatorExcl2 = ageAndGenderFilter24To64Df.as("df1").join(unionOfDinoExclAndYearDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).select("df1.member_sk")

    val finalDinoExclusion = finalDinoExclusion1Df.union(dinominatorExcl2) /*dx*/

    val finalDinominatorDf = dinominatorDf.except(finalDinoExclusion) /*D*/

    /*Dinominator Calculation ends*/

    /* Numerator Calculation Starts */

    /*Step 1 "Cervical Cytology" */

    val hedisJoinedForStep1CcsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, KpiConstants.ccsNumeratorStep1ValueSet, KpiConstants.ccsNumeratorStep1CodeSystem)
    val measurementForStep1CcsDf = UtilFunctions.mesurementYearFilter(hedisJoinedForStep1CcsDf, "start_date", year, KpiConstants.measurementYearLower, measurementYearUpper).select(KpiConstants.memberskColName,"start_date")
    val finalStep1NumeratorDf = measurementForStep1CcsDf.intersect(ageAndGenderFilter24To64Df)

    /*Step 2 "HPV test " */

    val hedisJoinedForStep2CcsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ccsMeasureId, KpiConstants.ccsNumeratorStep2ValueSet, KpiConstants.ccsNumeratorStep2CodeSystem)
    val measurementForStep2CcsDf = UtilFunctions.mesurementYearFilter(hedisJoinedForStep2CcsDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measurementFourYearUpper).select(KpiConstants.memberskColName,"start_date")

    /*"Cervical Cytology" with HPV test having date difference as 4 days */

    val numeratorForStep2CcsDf = measurementForStep1CcsDf.as("df1").join(measurementForStep2CcsDf.as("df2"),(measurementForStep1CcsDf.col(KpiConstants.memberskColName) === measurementForStep2CcsDf.col(KpiConstants.memberskColName)) &&  (abs(datediff(measurementForStep1CcsDf.col("start_date"), measurementForStep2CcsDf.col("start_date"))) .<= (4)), KpiConstants.innerJoinType).select(KpiConstants.memberskColName)

    val finalStep2NumeratorDf = numeratorForStep2CcsDf.intersect(ageAndGenderFilter30To64Df)

    /* Step3 Sum the events from steps 1 and 2 to obtain the rate */

    val numeratorDf = finalStep1NumeratorDf.union(finalStep2NumeratorDf).select("df1.member_sk")

    /* Final Numerator after intersection with dinominator */

    val finalNumeratorDf = numeratorDf.intersect(finalDinominatorDf)
    finalNumeratorDf.show()

    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.ccsNumeratorStep1ValueSet ::: KpiConstants.ccsNumeratorStep2ValueSet
    val dinoExclReasonValueSet = KpiConstants.ccsDinomenatorExclCodeSystem
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cisDtpaMeasureId)

    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, finalDinoExclusion, finalNumeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+factGapsInHedisTblName)


    spark.sparkContext.stop()
  }

}
