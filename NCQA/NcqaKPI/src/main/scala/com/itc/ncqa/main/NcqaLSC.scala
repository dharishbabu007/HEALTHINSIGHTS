package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object NcqaLSC {

  def main(args: Array[String]): Unit = {


    //<editor-fold desc="Reading Program arguments and SparkSession Object Creation.">

    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    val measureId = args(4)
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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQALSC")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Laoding Required tables to memory">

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val ref_lobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Allowable Gap,Age filter and Continous enrollment">

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,ref_lobDf,dimFacilityDf,lob_name,KpiConstants.lscMeasureTitle)

    var lookUpDf = spark.emptyDataFrame

    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }


    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*Age filter*/

    val expr = "year(date_add(dob,730)) = "+year
    val ageFilterDf = commonFilterDf.filter(expr)

    /*Adding first_dob,second_dob columns for continous enrollment check.*/
    val firstSecondDobAddedDf = ageFilterDf.withColumn("first_dob",add_months(ageFilterDf.col(KpiConstants.dobColName),12)).withColumn("second_dob",add_months(ageFilterDf.col(KpiConstants.dobColName),24))

    /*Continous Enroll check(continously enrolled in the 12 months prior to the second birthday)*/
    val continuousEnrollDf = firstSecondDobAddedDf.filter(firstSecondDobAddedDf.col(KpiConstants.memStartDateColName).<=(firstSecondDobAddedDf.col("first_dob")) && firstSecondDobAddedDf.col(KpiConstants.memEndDateColName).>=(firstSecondDobAddedDf.col("second_dob"))).drop("first_dob")
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*Dinominator calculation*/
    val dinominatorDf = continuousEnrollDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    //dinominatorAfterExclusionDf.show()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator Calculation*/
    val leadTestValList = List(KpiConstants.leadTestVal)
    val leadTestCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.loincCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForNumerator = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.lsMeasureId,leadTestValList,leadTestCodeSystem)

    /*Lead test on or before the second birth day .*/
    val leadtestbeforeSecBdayDf = continuousEnrollDf.select("member_sk","second_dob").as("df1").join(hedisJoinedForNumerator.as("df2"),$"df1.member_sk" === $"df2.member_sk",KpiConstants.innerJoinType)
                                       .filter($"start_date".<=($"second_dob")).select("df1.member_sk")
    val numeratorDf = leadtestbeforeSecBdayDf.intersect(dinominatorAfterExclusionDf)
    //dayFilterCondionNumeratorDf.show()
    numeratorDf.show()
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = leadTestValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, hospiceDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto("ncqa_sample.gaps_in_hedis_test")*/
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()
  }

}
