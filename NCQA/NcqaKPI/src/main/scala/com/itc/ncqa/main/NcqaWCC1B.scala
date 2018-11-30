package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaWCC1B {



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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaWCC1B")
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

    /*Age 3–11 (Nutrition Counseling)*/

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age3Val, KpiConstants.age11Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)


    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Dinominator Calculation ((Outpatient Value Set during year)*/
    val hedisJoinedForDinominator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.wcc1bMeasureId, KpiConstants.abavalueSetForDinominator, KpiConstants.abscodeSystemForDinominator)

    hedisJoinedForDinominator.printSchema()
    /*Dinominator with a PCP or an OB/GYN = yes*/


    val hedisJoinedForDinoWithPCP = dimProviderDf.as("df1").join(hedisJoinedForDinominator.as("df2"), ($"df1.provider_sk" === $"df2.provider_sk")  , KpiConstants.innerJoinType).filter($"df1.pcp" === KpiConstants.yesVal || $"df1.obgyn" === KpiConstants.yesVal )

    val measurement = UtilFunctions.mesurementYearFilter(hedisJoinedForDinoWithPCP, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("member_sk").distinct()
    val dinominatorForOutput = ageFilterDf.as("df1").join(measurement.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.member_sk", "df1.product_plan_sk", "df1.quality_measure_sk", "df1.facility_sk")
    val dinominator = ageFilterDf.as("df1").join(measurement.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.member_sk").distinct()

    /*Dinominator Exclusion1 (Pregnancy Value Set during year )*/
    val joinForDinominatorExcl = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.wcc1bMeasureId, KpiConstants.abavaluesetForDinExcl, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementExcl = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion2 (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val unionOfDinoExclsionsDf = measurementExcl.union(hospiceDinoExclDf)
    val dinominatorExcl = ageFilterDf.as("df1").join(unionOfDinoExclsionsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).select("df1.member_sk")

    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val finalDinominatorDf = dinominator.except(dinominatorExcl)
    finalDinominatorDf.show()



    /*Numerator1 Calculation (Nutrition Counseling  Value Set )*/

    /*Numerator1 Calculation (Nutrition Counseling  Value Set ) for primaryDiagnosisCodeSystem */
    val joinForNumeratorForNutrition1 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.wcc1bMeasureId, KpiConstants.wcc1bNutritionValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementNumeratorNutrition1 = UtilFunctions.mesurementYearFilter(joinForNumeratorForNutrition1, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val numeratorNutritionDf1 = ageFilterDf.as("df1").join(measurementNumeratorNutrition1.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratorNutrition1.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*Numerator1 Calculation (Nutrition Counseling  Value Set ) for primaryDiagnosisCodeSystem */
    val joinForNumeratorForNutrition2 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.wcc1bMeasureId, KpiConstants.wcc1bNutritionValueSet, KpiConstants.wcc1bNutritionCodeSystem)
    val measurementNumeratorNutrition2 = UtilFunctions.mesurementYearFilter(joinForNumeratorForNutrition2, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val numeratorNutritionDf2 = ageFilterDf.as("df1").join(measurementNumeratorNutrition2.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratorNutrition2.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    val numeratorNutritionDf = numeratorNutritionDf1.union(numeratorNutritionDf2)

    /*Final Numerator(Elements who are present in dinominator and numerator)*/
    val wcc1bnumeratorFinalDf = numeratorNutritionDf.intersect(finalDinominatorDf).select(KpiConstants.memberskColName).distinct()

    wcc1bnumeratorFinalDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.wcc1bNutritionValueSet
    val dinominatorExclValueSet = KpiConstants.abavaluesetForDinExcl
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.wcc1bMeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorForOutput, dinominatorExcl, wcc1bnumeratorFinalDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)



    /*Data populating to fact_hedis_qms*/
    //   val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.WCCMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    //   val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    //   val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //  outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    spark.sparkContext.stop()
  }


}
