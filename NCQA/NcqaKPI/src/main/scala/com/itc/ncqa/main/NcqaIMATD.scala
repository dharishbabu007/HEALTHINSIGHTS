package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaIMATD {

  def main(args: Array[String]): Unit = {

    /*Reading the program arguments*/

    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source =""

    /*define data_source on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }


    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    /*define data_source based on program type. */
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIMATD")
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

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cisMeasureTitle)

    var lookUpDf = spark.emptyDataFrame

    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 10 and 13 */
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age10Val, KpiConstants.age13Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*Dinominator calculation*/

    val ageFilterForDinoDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age0Val, KpiConstants.age13Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    val dinominatorDf = ageFilterForDinoDf

    dinominatorDf.show()

    /*Dinominator Exclusion1 (Anaphylactic Reaction Due To Vaccination or Serum Exclusion)*/

    val joinForDinominatorExcl = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.ImatdMeasureId, KpiConstants.cisImamenDinoExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementExcl = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion2 (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val unionOfDinoExclsionsDf = measurementExcl.union(hospiceDinoExclDf)
    val dinominatorExcl = ageFilterDf.as("df1").join(unionOfDinoExclsionsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).select("df1.member_sk")

    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val finalDinominatorDf = dinominatorDf.select("df1.member_sk").except(dinominatorExcl)

    finalDinominatorDf.show()

    /*Numerator1 Calculation (IMATD screening or monitoring test)*/

    val hedisJoinedForImatdScreeningDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.ImatdMeasureId, KpiConstants.cisImatdValueSet, KpiConstants.cisImatdCodeSystem)
    val measurement = UtilFunctions.mesurementYearFilter(hedisJoinedForImatdScreeningDf, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val ageFilterJoinNumeratorDf = ageFilterDf.as("df1").join(measurement.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(measurement.col(KpiConstants.startDateColName).isNotNull).select(ageFilterDf.col(KpiConstants.memberskColName),ageFilterDf.col(KpiConstants.dobColName),measurement.col(KpiConstants.startDateColName))

    /*vaccination administered only after 11 years of days of birth*/

    val cisImatdJoinDf = ageFilterJoinNumeratorDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "inner").select("df2.member_sk", "df2.start_date_sk")

    /* Filtering members with At least three IMATD vaccinations */

    val cisImatdCountDf = cisImatdJoinDf.groupBy("member_sk").agg(count("start_date_sk").alias("count1")).filter($"count1".>=(1)).select(KpiConstants.memberskColName)

    val cisImatdNumeratorDf = cisImatdCountDf.intersect(finalDinominatorDf)

    cisImatdNumeratorDf.show()


    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.cisImatdValueSet
    val dinoExclReasonValueSet = KpiConstants.cisImamenDinoExclValueSet
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source, KpiConstants.ImatdMeasureId)

    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExcl, cisImatdNumeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+factGapsInHedisTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.ImamenMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+factHedisQmsTblName)spark.sparkContext.stop()
    spark.sparkContext.stop()

  }


}
