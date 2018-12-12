package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.DataLoadFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NcqaHPDIEBS {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAABA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)

    val memberAndMembershipJoinDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"),dimMemberDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
        .select("df1.member_sk",KpiConstants.arrayofColumn1:_*)


    /*Load the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val memStartDateAddedDf = memberAndMembershipJoinDf.as("df1").join(dimDateDf.as("df2"), memberAndMembershipJoinDf.col(KpiConstants.memPlanStartDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_start_temp").drop(KpiConstants.memPlanStartDateSkColName)
    val memEndDateAddedDf = memStartDateAddedDf.as("df1").join(dimDateDf.as("df2"), memStartDateAddedDf.col(KpiConstants.memPlanEndDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_end_temp").drop(KpiConstants.memPlanEndDateSkColName)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = memEndDateAddedDf.withColumn(KpiConstants.memStartDateColName, to_date($"mem_start_temp", "dd-MMM-yyyy")).withColumn(KpiConstants.memEndDateColName, to_date($"mem_end_temp", "dd-MMM-yyyy")).drop("dob_temp","mem_start_temp","mem_end_temp")
    //resultantDf.printSchema()

    val start_date = year + "-01-01"
    val end_date = year +"-12-31"
    val continiousEnrollDf = resultantDf.filter(resultantDf.col(KpiConstants.memStartDateColName).<=(start_date) &&(resultantDf.col(KpiConstants.memEndDateColName).>=(end_date)))
    //continiousEnrollDf.select(KpiConstants.memberskColName,KpiConstants.memStartDateColName,KpiConstants.memEndDateColName).show(100)

    /*state wise member elements who has continous membership enrollment*/
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName).select(KpiConstants.lobIdColName,KpiConstants.lobNameColName)
    val lobNameAddedDf = continiousEnrollDf.as("df1").join(refLobDf.as("df2"),continiousEnrollDf.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName),KpiConstants.innerJoinType).select("df1.*","df2.lob_name")
    val statewiseCountDf = lobNameAddedDf.groupBy(KpiConstants.stateColName,KpiConstants.lobNameColName).agg(countDistinct(KpiConstants.memberskColName).alias(KpiConstants.countColName))
    statewiseCountDf.show(100)
  }
}
