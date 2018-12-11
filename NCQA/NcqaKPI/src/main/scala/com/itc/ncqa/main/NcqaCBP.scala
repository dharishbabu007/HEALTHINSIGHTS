package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaCBP {

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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaCBP")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    //print("counts:"+dimMemberDf.count()+","+factClaimDf.count()+","+factMembershipDf.count())

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.WCCMeasureTitle)
    //initialJoinedDf.show(50)

    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }


    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*Age 18–85 (BMI percentile)*/

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age85Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)


    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf.select("df1.member_sk")

    /*Dinominator Exclusion1 calculation*/
    /*Dinominator Calculation (for primaryDiagnosisCodeSystem with valueset "Frailty"  )*/

    val joinForDinominator1 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinoExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDinominator1 = UtilFunctions.mesurementYearFilter(joinForDinominator1, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("df1.member_sk")

    /*Dinominator Calculation (for proceedureCodeSystem with valueset "Frailty" )*/

    val joinForDinominator2 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinoExclValueSet, KpiConstants.cbpDinoCodeSystem)
    val measurementDinominator2 = UtilFunctions.mesurementYearFilter(joinForDinominator2, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("df1.member_sk")

    val measurementExcl = measurementDinominator1.union(measurementDinominator2)

    /*Dinominator Exclusion2 (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val unionOfDinoExclsionsDf = measurementExcl.union(hospiceDinoExclDf)
    val ageFilterDfForDinoExcl = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age81Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    val dinominatorExcl = ageFilterDfForDinoExcl.as("df1").join(unionOfDinoExclsionsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).select("df1.member_sk")

    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val finalDinominatorDf = dinominatorDf.except(dinominatorExcl)

    finalDinominatorDf.show()



    /*Numerator1a Calculation (for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"  )*/


    val joinForNumerator1a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpCommonNumeratorValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementNumerator1a = UtilFunctions.mesurementYearFilter(joinForNumerator1a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val numerator1aDf = ageFilterDf.as("df1").join(measurementNumerator1a.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumerator1a.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*Numerator1b Calculation (for proceedureCodeSystem with valueset "Outpatient","Telehealth Modifier" combination )*/

    val joinForNumerator1b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumerator1ValueSet, KpiConstants.cbpNumerator1CodeSystem)
    val measurementNumerator1b = UtilFunctions.mesurementYearFilter(joinForNumerator1b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val numerator1bDf = ageFilterDf.as("df1").join(measurementNumerator1b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumerator1b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")


    val numerator1Df = numerator1aDf.union(numerator1bDf)


    /*Numerator2b Calculation (for proceedureCodeSystem with valueset "Telephone Visits" )*/

    val joinForNumerator2b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumerator2ValueSet, KpiConstants.cbpNumerator2CodeSystem)
    val measurementNumerator2b = UtilFunctions.mesurementYearFilter(joinForNumerator2b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val numerator2bDf = ageFilterDf.as("df1").join(measurementNumerator2b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumerator2b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*same as numerator1 for for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"*/

    val numerator2Df = numerator1aDf.union(numerator2bDf)


    /*Numerator3b Calculation (for proceedureCodeSystem with valueset "Telephone Visits" )*/

    val joinForNumerator3b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumerator3ValueSet, KpiConstants.cbpNumerator3CodeSystem)
    val measurementNumerator3b = UtilFunctions.mesurementYearFilter(joinForNumerator3b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val numerator3bDf = ageFilterDf.as("df1").join(measurementNumerator3b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumerator3b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*same as numerator1 for for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"*/

    val numerator3Df = numerator1aDf.union(numerator3bDf)

    val finalNumerator = numerator1Df.union(numerator2Df).union(numerator3Df)

    /*Final Numerator(Elements who are present in dinominator and numerator)*/

    val cbpFinalNumeratorDf = finalNumerator.intersect(finalDinominatorDf).select(KpiConstants.memberskColName).distinct()

    cbpFinalNumeratorDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.cbpCommonNumeratorValueSet ::: KpiConstants.cbpNumerator1ValueSet ::: KpiConstants.cbpNumerator2ValueSet ::: KpiConstants.cbpNumerator3ValueSet
    val dinominatorExclValueSet = KpiConstants.cbpDinoExclValueSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.abaMeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExcl, cbpFinalNumeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)



    /*Data populating to fact_hedis_qms*/
    //   val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.WCCMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    //   val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    //   val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //  outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    spark.sparkContext.stop()
  }

}
