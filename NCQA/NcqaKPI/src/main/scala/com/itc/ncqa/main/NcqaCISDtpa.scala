package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaCISDtpa {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIPV")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val ref_lobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,ref_lobDf,dimFacilityDf,lob_name,KpiConstants.cisMeasureTitle)

    var lookUpDf = spark.emptyDataFrame

    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }

    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    commonFilterDf.printSchema()

    /*Age filter*/

    val expr = "year(date_add(dob,730)) = "+year
    val ageFilterDf = commonFilterDf.filter(expr)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)


    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    dinominatorDf.show()


    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val dinoExclDf = hospiceDf.select(KpiConstants.memberskColName)

    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(dinoExclDf)
    dinominatorAfterExclusionDf.show()

    /*Numerator Calculation*/

    /*Numerator1 Calculation (DTaP screening or monitoring test)*/

    val hedisJoinedForDTaPScreeningDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisDtpaMeasureId, KpiConstants.cisDtpaValueSet, KpiConstants.cisDtpaCodeSystem)
    val ageFilterJoinNumeratorDf = ageFilterDf.as("df1").join(hedisJoinedForDTaPScreeningDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === hedisJoinedForDTaPScreeningDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(hedisJoinedForDTaPScreeningDf.col(KpiConstants.startDateColName).isNotNull).select(ageFilterDf.col(KpiConstants.memberskColName),ageFilterDf.col(KpiConstants.dobColName),hedisJoinedForDTaPScreeningDf.col(KpiConstants.startDateColName))
    val dayFilterCondionNumeratorDf = ageFilterJoinNumeratorDf. filter(datediff(date_add(commonFilterDf.col(KpiConstants.dobColName),730),ageFilterJoinNumeratorDf.col(KpiConstants.startDateColName)).>=(0))


    /*vaccination administered only after 42 days of birth*/

    val dayFilter42NumeratorDf = dayFilterCondionNumeratorDf.filter(datediff(date_add(commonFilterDf.col(KpiConstants.dobColName),42),dayFilterCondionNumeratorDf.col(KpiConstants.startDateColName)).>=(0))

    val cisDtapJoinDf = dayFilterCondionNumeratorDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "inner").select("df2.member_sk", "df2.start_date_sk")

    /* Filtering members with At least four DTaP vaccinations */

    val cisDtapCountDf = cisDtapJoinDf.groupBy("member_sk").agg(count("start_date_sk").alias("count1")).filter($"count1".>=(4)).select("df2.member_sk")


    val cisDtapNumeratorDf = cisDtapCountDf.intersect(dinominatorAfterExclusionDf)

    cisDtapNumeratorDf.show()

    /*common output creation(data to fact_gaps_in_hedis table)*/
    val numeratorReasonValueSet = KpiConstants.cisDtpaValueSet
    val dinoExclReasonValueSet = KpiConstants.emptyList
    val numExclReasonValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorReasonValueSet, dinoExclReasonValueSet, numExclReasonValueSet)
    val sourceAndMsrList = List(data_source,KpiConstants.cisDtpaMeasureId)

    val numExclDf = spark.emptyDataFrame
    val commonOutputFormattedDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinoExclDf, cisDtapNumeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    //commonOutputFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+factGapsInHedisTblName)


    /*common output creation2 (data to fact_hedis_qms table)*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.cisMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select(KpiConstants.memberskColName, KpiConstants.lobIdColName)
    val qmsoutFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //qmsoutFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(KpiConstants.dbName+"."+factHedisQmsTblName)spark.sparkContext.stop()


  }

}