package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object NcqaAWC {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAPOC")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val year = args(0)
    val lob_id = args(1)

    import spark.implicits._


    var lookupTableDf = spark.emptyDataFrame
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





    // val arrayOfColumn = List("member_id","date_of_birth_sk","gender","primary_diagnosis","procedure_code","start_date_sk","PROCEDURE_CODE_MODIFIER1","PROCEDURE_CODE_MODIFIER2","PROCEDURE_HCPCS_CODE","CPT_II","CPT_II_MODIFIER","DIAGNOSIS_CODE_2","DIAGNOSIS_CODE_3","DIAGNOSIS_CODE_4","DIAGNOSIS_CODE_5","DIAGNOSIS_CODE_6","DIAGNOSIS_CODE_7","DIAGNOSIS_CODE_8","DIAGNOSIS_CODE_9","DIAGNOSIS_CODE_10")
    val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk",KpiConstants.arrayOfColumn:_*)
    //joinedDimMemberAndFctclaimDf.printSchema()
    val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.*","df2.product_plan_sk").withColumnRenamed("start_date_sk","claim_start_date_sk")

    val ref_lobDf = spark.sql(KpiConstants.refLobLoadQuery)



    if(args(1).equals("Commercial"))
    {
      lookupTableDf = spark.sql(KpiConstants.view45DaysLoadQuery)
    }
    else
    {
      lookupTableDf = spark.sql(KpiConstants.view60DaysLoadQuery)
    }

    /*common filter checking*/
    val commonFilterDf = joinedFactMembershipDf.as("df1").join(lookupTableDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","left_outer").filter("start_date is null").select("df1.*")

    val dimdateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    val dobDateValAddedDf = commonFilterDf.as("df1").join(dimdateDf.as("df2"),$"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*",$"df2.calendar_date").withColumnRenamed("calendar_date","dob_temp").drop("DATE_OF_BIRTH_SK")
    val dateTypeDf = dobDateValAddedDf.withColumn("dob", to_date($"dob_temp","dd-MMM-yyyy")).drop("dob_temp")





    /*loading ref_hedis table*/
    val refHedisDf = spark.sql(KpiConstants.refHedisLoadQuery)

    /*Dinominator (Allowable age group)*/
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf,"dob",year,"12","21",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal).select("member_sk")


    /*Numerator1 (Well-Care Value Set as procedure code with PCP or an OB/GYN practitioner during the measurement year)*/
    val hedisJoinedForWcvAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.awcMeasureId,KpiConstants.awcWcvValueSet,KpiConstants.awcWcvCodeSystem)
    val dimProviderDf_init = spark.sql(KpiConstants.dimProviderLoadQuery)
    val dimProviderDfColumns = dimProviderDf_init.columns.map(f => f.toUpperCase)
    val dimProviderDf = UtilFunctions.removeHeaderFromDf(dimMemberDf_init, dimProviderDfColumns, "provider_sk")
    val joinedWithDimProviderDf = hedisJoinedForWcvAsProDf.as("df1").join(dimProviderDf.as("df2"),$"df1.provider_sk" === $"df2.provider_sk","inner").filter($"df2.pcp".===("Y")  || ($"df2.obgyn".===("Y")))
    val measurementForWcvAsProDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderDf,"start_date",year,0,365).select("member_sk").distinct()



    /*Numerator2 (Well-Care Value Set as primary diagnosis with PCP or an OB/GYN practitioner during the measurement year)*/
    val hedisJoinedForWcvAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.awcMeasureId,KpiConstants.awcWcvValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val joinedWithDimProviderAsDiagDf = hedisJoinedForWcvAsDiagDf.as("df1").join(dimProviderDf.as("df2"),$"df1.provider_sk" === $"df2.provider_sk","inner").filter($"df2.pcp".===("Y")  || ($"df2.obgyn".===("Y")))
    val measurementForWcvAsDiagDf = UtilFunctions.mesurementYearFilter(joinedWithDimProviderAsDiagDf,"start_date",year,0,365).select("member_sk").distinct()
    val awcNumeratorUnionDf = measurementForWcvAsProDf.union(measurementForWcvAsDiagDf).distinct()
    val awcNumeratorDf = awcNumeratorUnionDf.intersect(ageFilterDf).distinct()


    val measurementDinominatorExclDf = spark.emptyDataFrame
    val lobdetailsData = ageFilterDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")
    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,measurementDinominatorExclDf,awcNumeratorDf,dimMemberDf,KpiConstants.advMeasureId)
    formattedOutPutDf.orderBy("MemID").show()
  }
}
