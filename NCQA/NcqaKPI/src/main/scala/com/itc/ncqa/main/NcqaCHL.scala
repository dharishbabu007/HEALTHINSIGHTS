/*
package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

object NcqaCHL {

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

    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf,"dob",year,"16","24",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    //println("The ageFilterDf count is :"+ageFilterDf.count())
    val genderFilterDf =ageFilterDf.filter($"gender".===("F")).select("member_sk","dob")




    //genderFilterDf.show(50)
    /*Dinominator Calculation Starts*/

    /*Sexual Activity,Pregnancy dinominator Calculation*/
   /* val firstValueSet = List("Sexual Activity","Pregnancy")
    val codeSytemForPrimaryDiagnosis = List("ICD%")*/
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.chlMeasureId,KpiConstants.chlSexualActivityValueSet,KpiConstants.chlSexualActivitycodeSytem)     //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.primary_diagnosis" === $"df3.code","inner").filter($"measureid".===("CHL").&&($"valueset".isin(firstValueSet:_*).&&($"codesystem".like("ICD%")))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem")
    val sexualAndPregnancyMeasurementDf = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino,"start_date",year,0,730).select("member_sk","start_date")
    val sexualAndPregnancyDistinctDf = sexualAndPregnancyMeasurementDf.select("member_sk").distinct()


    /*Pregnancy Tests Dinominator Calculation*/
    /*val codeSystem = List("CPT","HCPCS","UBREV")
    val pregnancyValueSet = List("Pregnancy Tests")*/
    val hedisJoinedForSecondDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.chlMeasureId,KpiConstants.chlpregnancyValueSet,KpiConstants.chlpregnancycodeSystem)     //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code","inner").filter($"measureid".===("CHL").&&($"valueset".===("Pregnancy Tests")).&&($"codesystem".isin(codeSystem:_*))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem")
    val pregnancyTestMeasurementDf = UtilFunctions.mesurementYearFilter(hedisJoinedForSecondDino,"start_date",year,0,730).select("member_sk","start_date")
    val pregnancyTestDistinctDf = pregnancyTestMeasurementDf.select("member_sk").distinct()




    /*Pharmacy data Dinominator*/
    val ref_medvaluesetDf = spark.sql("select * from healthin.ref_med_value_set")
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"),$"df2.ndc_number" === $"df3.ndc_code","inner").filter($"measure_id".===("CHL")).select("df1.member_sk","df2.start_date_sk")
    val startDateValAddedDfForThirddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForThirdDino = startDateValAddedDfForThirddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val pharmacyMeasurementDf = UtilFunctions.mesurementYearFilter(dateTypeDfForThirdDino,"start_date",year,0,730).select("member_sk","start_date")
    val pharmacyDistinctDf = pharmacyMeasurementDf.select("member_sk").distinct()

    /*union of three different dinominator calculation*/
    val unionOfThreeDinominatorDf = sexualAndPregnancyDistinctDf.union(pregnancyTestDistinctDf).union(pharmacyDistinctDf)
    val dinominatorDf = genderFilterDf.as("df1").join(unionOfThreeDinominatorDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk").distinct()
   // dinominatorDf.show()
    /*End of dinominator Calculation */




    /*Dinominator Exclusion Starts*/

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)

    /*val valueSetForPregnancyExclusion = List("Pregnancy Test Exclusion")
    val codeSystemForPregnancyExclusion = List("CPT","HCPCS","UBREV")*/

    /*pregnancy test Exclusion during measurement period*/

    val hedisJoinedForPregnancyExclusion = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.chlMeasureId,KpiConstants.chlPregnancyExclusionvalueSet,KpiConstants.chlPregnancyExclusioncodeSystem)    //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code","inner").filter($"measureid".===("CHL").&&($"valueset".===("Pregnancy Test Exclusion")).&&($"codesystem".isin(codeSystem:_*))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem")
    val measurementDfPregnancyExclusion = UtilFunctions.mesurementYearFilter(hedisJoinedForPregnancyExclusion,"start_date",year,0,730).select("member_sk").distinct()

    /*isotretinoin filter*/
    val medValuesetForIsoExcl = pregnancyTestMeasurementDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"),$"df2.ndc_number" === $"df3.ndc_code","inner").filter($"measure_id".===("CHL")).select("df1.*","df2.start_date_sk")
    val startDateValAddedDfForIsoExcl = medValuesetForIsoExcl.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForIsoExcl = startDateValAddedDfForIsoExcl.withColumn("rx_start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    dateTypeDfForIsoExcl.printSchema()
   // val dayFilteredDfForIsoExcl = dateTypeDfForIsoExcl.filter(datediff(dateTypeDfForIsoExcl.col("rx_start_date"),dateTypeDfForIsoExcl.col("start_date")).===(0) || datediff(dateTypeDfForIsoExcl.col("rx_start_date"),dateTypeDfForIsoExcl.col("start_date")).===(6))).select("member_sk").distinct()

    val dayFilteredDfForIsoExcl = dateTypeDfForIsoExcl.filter((datediff(dateTypeDfForIsoExcl.col("rx_start_date"), dateTypeDfForIsoExcl.col("start_date")).===(0)) ||  ( datediff(dateTypeDfForIsoExcl.col("rx_start_date"), dateTypeDfForIsoExcl.col("start_date")).===(6)))
    /*pregnancyExclusion and isotretinoinDf join*/
    val prgnancyExclusionandIsoDf = measurementDfPregnancyExclusion.as("df1").join(dayFilteredDfForIsoExcl.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk").distinct()


    /*Diagnostic Radiology Value Set Exclusion*/
    val hedisJoinedForDigRadExclusion = pregnancyTestMeasurementDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code","inner").filter($"measureid".===(KpiConstants.chlMeasureId).&&($"valueset".===("Diagnostic Radiology")).&&($"codesystem".like("CPT"))).select("df1.*","df2.start_date_sk")
    val startDateValAddedDfDigRadExclusion = hedisJoinedForDigRadExclusion.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfDigRadExclusion = startDateValAddedDfDigRadExclusion.withColumn("dig_start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop( "start_temp")
    val dayFilterdDigRadExclusion = dateTypeDfDigRadExclusion.filter(datediff(dateTypeDfDigRadExclusion.col("dig_start_date"),dateTypeDfDigRadExclusion.col("start_date")).===(0) ||(datediff(dateTypeDfDigRadExclusion.col("dig_start_date"),dateTypeDfDigRadExclusion.col("start_date")).===(6))).select("member_sk").distinct()

    /*pregnancyExclusion and Diagnostic Radiology join*/
    val pregnancyExclusionandDigRadDf = measurementDfPregnancyExclusion.as("df1").join(dayFilterdDigRadExclusion.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk").distinct()

    /*prgnancyExclusionandIsoDf and pregnancyExclusionandDigRadDf union */
    val unionOfExclusionDf = prgnancyExclusionandIsoDf.union(pregnancyExclusionandDigRadDf).distinct()


    /*join of pregnancy test Df and prgnancyExclusionandIsoDf and pregnancyExclusionandDigRadDf union*/
    val secondDinominatorExclusion = pregnancyTestDistinctDf.as("df1").join(unionOfExclusionDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk").distinct()

    /*union of hospice and the secondDinominatorExclusion*/
    val dinominatorExclusionDf = hospiceDf.union(secondDinominatorExclusion).distinct()
    /*End of Dinominator Exclusion Starts*/


    /*Numerator calculation*/
    //val codeSystemForNum = List("CPT","HCPS","LOINC")
    val hedisJoinedForNumerator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.chlMeasureId,KpiConstants.chlchalmdiaValueSet,KpiConstants.chlChalmdiacodeSystem)
    val measurementDfForNumerator = UtilFunctions.mesurementYearFilter(hedisJoinedForNumerator,"start_date",year,0,730).select("member_sk").distinct()
    val numeratorDistinctDf = genderFilterDf.as("df1").join(measurementDfForNumerator.as("df2"),$"df1.member_sk" === $"df2.member_sk","right_outer").filter($"df1.member_sk".isNotNull).select("df1.member_sk").distinct()
    numeratorDistinctDf.show()




    /*output format creation*/
    /*val lobdetailsData = dinominatorDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")
    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,dinominatorExclusionDf,numeratorDistinctDf,dimMemberDf,"CHL")
    formattedOutPutDf.show()*/
  }

}
*/
