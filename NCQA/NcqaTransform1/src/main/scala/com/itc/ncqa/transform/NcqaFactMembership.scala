package com.itc.ncqa.transform

import com.itc.ncqa.utils.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, current_timestamp, date_format, lit,abs}

object NcqaFactMembership {

  def main(args: Array[String]): Unit = {
    println("started")


    val config = new SparkConf().setAppName("Ncqa FactMembershipCreation").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    val schemaPath = args(0)

    import spark.implicits._
    val schemaRdd = spark.sparkContext.textFile(schemaPath,1)

    val queryString = "select * from ncqa_intermediate.membership_enrollment"
    val membershipEnrollmentDf = spark.sql(queryString)
    /*membershipEnrollmentDf.printSchema()
    membershipEnrollmentDf.show()*/

    /*formatting the date columns into dd-mm-yyyy */
    val dateFomrattedFactMembershipDf = membershipEnrollmentDf.withColumn("startdate",date_format(membershipEnrollmentDf.col("startdate"),"dd-MMM-yyyy")).withColumn("finishdate",date_format(membershipEnrollmentDf.col("finishdate"),"dd-MMM-yyyy"))

    /*join the Df with different table for getting the sk*/
    val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val StartDateskAddedFactMembershipDf = dateFomrattedFactMembershipDf.as("df1").join(dimDateDf.as("df2"),$"df1.startdate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","MEMBER_PLAN_START_DATE_SK").drop("startdate")
    val FinshDateSkAddedFactMembershipDf = StartDateskAddedFactMembershipDf.as("df1").join(dimDateDf.as("df2"),$"df1.finishdate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","MEMBER_PLAN_END_DATE_SK").drop("finishdate")
    /*load the REF_LOB data from hive*/
    val refLobDf = spark.sql("select * from ncqa_sample.ref_lob")
    val PayerSkAddedFactMembershipDf = FinshDateSkAddedFactMembershipDf.as("df1").join(refLobDf.as("df2"),$"df1.payer" === $"df2.lob_name").select($"df1.*",$"df2.lob_id").withColumnRenamed("lob_id","LOB_ID").drop("payer")
    val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
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
    formattedFactMembershipDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_membership")


  }
}
