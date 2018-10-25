package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, dayofyear, expr, lit, to_date, when}

object NcqaLS {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAPOC")
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
    val ref_lobDf = spark.sql(KpiConstants.refLobLoadQuery)


    val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "primary_diagnosis", "procedure_code", "start_date_sk" /*"PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
    val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.member_sk", KpiConstants.arrayOfColumn: _*)
    val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.*", "df2.product_plan_sk", "df2.lob_id")


    val dimdateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dobDateValAddedDf = joinedFactMembershipDf.as("df1").join(dimdateDf.as("df2"), $"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "dob_temp").drop("DATE_OF_BIRTH_SK")
    val dateTypeDf = dobDateValAddedDf.withColumn("dob", to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")

    /*doing age filter */
    val expr = "year(date_add(dob,730)) = "+year
    val ageFilterDf = dateTypeDf.filter(expr)
    //ageFilterDf.select("member_sk","dob").show()

    /*load ref_hedis table*/
    val refHedisDf = spark.sql("select * from ncqa_sample.REF_HEDIS2016")
    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    //hospiceDf.show()
    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf.select("member_sk").distinct()
    //dinominatorDf.show()

    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorDf.except(hospiceDf)

    //dinominatorAfterExclusionDf.show()

    /*Numerator Calculation*/
   /* val codeSystemForNum = List("CPT","LOINC","HCPCS")
    val valueSetForNumerator =List("Lead Tests")*/
    val hedisJoinedForNumerator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.lsMeasureId,KpiConstants.clsValueSetForNumerator,KpiConstants.clsCodeSystemForNum)  //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code","inner").filter($"measureid".===("LSC").&&($"valueset".===("Lead Tests")).&&($"codesystem".isin(codeSystemForNum:_*))).select("df1.member_sk","df2.start_date_sk")
    val ageFilterJoinNumeratorDf = ageFilterDf.as("df1").join(hedisJoinedForNumerator.as("df2"),$"df1.member_sk" === $"df2.member_sk","left_outer").filter($"df2.start_date".isNotNull).select("df1.member_sk","df1.dob","df2.start_date")
    val dayFilterCondionNumeratorDf = ageFilterJoinNumeratorDf.filter(datediff(date_add(ageFilterJoinNumeratorDf.col("dob"),730),ageFilterJoinNumeratorDf.col("start_date")).>=(0)).select("member_sk").distinct()
    dayFilterCondionNumeratorDf.show()






    /*Output Format*/
    val lobdetailsData = dinominatorDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")
    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,hospiceDf,dayFilterCondionNumeratorDf,dimMemberDf,"LSC")
    formattedOutPutDf.show()
  }
}
