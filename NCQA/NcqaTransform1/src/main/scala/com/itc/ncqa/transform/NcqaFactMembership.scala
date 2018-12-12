package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

object NcqaFactMembership {

  def main(args: Array[String]): Unit = {
    println("started")

    val schemaFilePath = args(0)
    val sourceDbName = args(1)
    val targetDbName = args(2)
    val kpiName = args(3)
    TransformConstants.setSourceDbName(sourceDbName)
    TransformConstants.setTargetDbName(targetDbName)

    val config = new SparkConf().setAppName("Ncqa FactMembershipCreation").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    import spark.implicits._
    val schemaRdd = spark.sparkContext.textFile(schemaFilePath,1)

   /* val queryString = "select * from ncqa_intermediate.membership_enrollment"
    val membershipEnrollmentDf = spark.sql(queryString)*/
    val membershipEnrollmentDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.membershipEnrollTableName,kpiName)
    /*membershipEnrollmentDf.printSchema()
    membershipEnrollmentDf.show()*/

    /*formatting the date columns into dd-mm-yyyy */
    val dateFomrattedFactMembershipDf = membershipEnrollmentDf.withColumn("startdate",date_format(membershipEnrollmentDf.col("startdate"),"dd-MMM-yyyy")).withColumn("finishdate",date_format(membershipEnrollmentDf.col("finishdate"),"dd-MMM-yyyy"))

    /*join the Df with different table for getting the sk*/
    //val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val StartDateskAddedFactMembershipDf = dateFomrattedFactMembershipDf.as("df1").join(dimDateDf.as("df2"),$"df1.startdate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","MEMBER_PLAN_START_DATE_SK").drop("startdate")
    val FinshDateSkAddedFactMembershipDf = StartDateskAddedFactMembershipDf.as("df1").join(dimDateDf.as("df2"),$"df1.finishdate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","MEMBER_PLAN_END_DATE_SK").drop("finishdate")
    /*load the REF_LOB data from hive*/
    //val refLobDf = spark.sql("select * from ncqa_sample.ref_lob")
    val refLobDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.refLobTableName)
    val PayerSkAddedFactMembershipDf = FinshDateSkAddedFactMembershipDf.as("df1").join(refLobDf.as("df2"),$"df1.payer" === $"df2.lob_name").select($"df1.*",$"df2.lob_id").withColumnRenamed("lob_id","LOB_ID").drop("payer")
    //val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
    val dimMemberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)
    val memberSkAddedFactMembershipDf = PayerSkAddedFactMembershipDf.as("df1").join(dimMemberDf.as("df2"),$"df1.memid" === $"df2.member_id").select($"df1.*",$"df2.member_sk").withColumnRenamed("member_sk","MEMBER_SK").drop("memid")


    /*creating column array that contains the details of newly adding columns*/
    val newcolumnarray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnAddedFactMembershipDf = UtilFunctions.addingColumnsToDataFrame(memberSkAddedFactMembershipDf,newcolumnarray)

    /*creating membership_sk from existing columns*/
    val memebershipskAddedFactMembershipDf = newlyColumnAddedFactMembershipDf.withColumn("MEMBERSHIP_SK",abs(lit(concat(lit($"MEMBER_SK"),lit($"MEMBER_PLAN_START_DATE_SK"),lit($"MEMBER_PLAN_END_DATE_SK"),lit($"PRODUCT_PLAN_SK"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))

    /*creating array of column names for ordering the Df*/
    val arrayOfColumns1 = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*formatting the df*/
    val formattedFactMembershipDf = memebershipskAddedFactMembershipDf.select(arrayOfColumns1.head,arrayOfColumns1.tail:_*)
    formattedFactMembershipDf.printSchema()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factMembershipTablename
    formattedFactMembershipDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)


  }
}
