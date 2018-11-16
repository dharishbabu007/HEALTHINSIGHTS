package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, dayofyear, expr, lit, to_date, when}

object NcqaLSC {



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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQALSC")
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

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)


    /*find out the hospice members*/

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()


    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()

    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)

    //dinominatorAfterExclusionDf.show()

    /*Numerator Calculation*/
    val hedisJoinedForNumerator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.lsMeasureId,KpiConstants.clsValueSetForNumerator,KpiConstants.clsCodeSystemForNum)
    val ageFilterJoinNumeratorDf = ageFilterDf.as("df1").join(hedisJoinedForNumerator.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === hedisJoinedForNumerator.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(hedisJoinedForNumerator.col(KpiConstants.startDateColName).isNotNull).select(ageFilterDf.col(KpiConstants.memberskColName),ageFilterDf.col(KpiConstants.dobColName),hedisJoinedForNumerator.col(KpiConstants.startDateColName))
    val dayFilterCondionNumeratorDf = ageFilterJoinNumeratorDf.filter(datediff(date_add(ageFilterJoinNumeratorDf.col("dob"),730),ageFilterJoinNumeratorDf.col("start_date")).>=(0)).select("member_sk").distinct()
    val numeratorDf = dayFilterCondionNumeratorDf.intersect(dinominatorAfterExclusionDf)
    //dayFilterCondionNumeratorDf.show()
    numeratorDf.show()



    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.clsValueSetForNumerator
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.lsMeasureId)


    val numExclDf = spark.emptyDataFrame

    /*val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, hospiceDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto("ncqa_sample.gaps_in_hedis_test")*/
    spark.sparkContext.stop()
  }


}
