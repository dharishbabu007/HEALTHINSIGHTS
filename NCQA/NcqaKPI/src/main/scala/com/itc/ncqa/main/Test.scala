package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object Test {


  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC2")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val year = args(0)
    val lob_id = args(1)
    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf_init = spark.sql(KpiConstants.dimMemberLoadQuery)
    val dimMemberDfColumns = dimMemberDf_init.columns.map(f => f.toUpperCase)
    val dimMemberDf = UtilFunctions.removeHeaderFromDf(dimMemberDf_init, dimMemberDfColumns, "member_sk")
    val factClaimDf_init = spark.sql(KpiConstants.factClaimLoadQuery)
    val factClaimDfColumns = factClaimDf_init.columns.map(f => f.toUpperCase)
    val factClaimDf = UtilFunctions.removeHeaderFromDf(factClaimDf_init, factClaimDfColumns, "member_sk")
    val factMembershipDf_init = spark.sql(KpiConstants.factMembershipLoadQuery)
    val factMembershipDfColumns = factMembershipDf_init.columns.map(f => f.toUpperCase())
    val factMembershipDf = UtilFunctions.removeHeaderFromDf(factMembershipDf_init, factMembershipDfColumns, "member_sk")
    val factRxClaimsDf_init = spark.sql(KpiConstants.factRxClaimLoadQuery)
    val factRxClaimsColumns = factRxClaimsDf_init.columns
    val factRxClaimsDf = UtilFunctions.removeHeaderFromDf(factRxClaimsDf_init,factRxClaimsColumns,"member_sk")
    val ref_lobDf = spark.sql(KpiConstants.refLobLoadQuery)



    //val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "primary_diagnosis", "procedure_code", "start_date_sk" /*"PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
    val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.member_sk", KpiConstants.arrayOfColumn: _*)
    val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.*", "df2.product_plan_sk", "df2.lob_id")


    /*load the look up view */
    val lookUpTableDf = spark.sql(KpiConstants.view45DaysLoadQuery)
    //lookUpTableDf.printSchema()



    /*Removing the elements who has a gap of 45 days*/
    val commonFilterDf = joinedFactMembershipDf.as("df1").join(lookUpTableDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "left_outer").filter("start_date is null").select("df1.*")

    /*converting dateofbirthsk to dob in date format*/
    val dimdateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    val dobDateValAddedDf = commonFilterDf.as("df1").join(dimdateDf.as("df2"), $"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "dob_temp").drop("DATE_OF_BIRTH_SK")
    val dateTypeDf = dobDateValAddedDf.withColumn("dob", to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf, "dob", year, "18", "75") .select("member_sk").distinct()
    //ageFilterDf.orderBy("member_sk").show(50)
  }
}
