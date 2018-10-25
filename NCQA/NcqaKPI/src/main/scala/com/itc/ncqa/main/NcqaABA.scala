package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

import scala.collection.JavaConversions._

object NcqaABA {


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
    //val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "primary_diagnosis", "procedure_code", "start_date_sk" /*"PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
    /*joined all the needed dataframes*/ //.filter($"df2.lob_id".===(lob_id))
    val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.member_sk", KpiConstants.arrayOfColumn:_*) //arrayOfColumn: _*)
    val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.*", "df2.product_plan_sk", "df2.lob_id")

    /*load the look up view */
    val lookUpTableDf = spark.sql(KpiConstants.view45DaysLoadQuery)
    //lookUpTableDf.printSchema()

    /*Removing the elements who has a gap of 45 days*/
    val commonFilterDf = joinedFactMembershipDf.as("df1").join(lookUpTableDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "left_outer").filter("start_date is null").select("df1.*")
    //commonFilterDf.printSchema()

    /*join with dim_date for getting the calender_date */
    val dimdateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    val dobDateValAddedDf = commonFilterDf.as("df1").join(dimdateDf.as("df2"), $"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "dob_temp").drop("DATE_OF_BIRTH_SK")
    val startDateValAddedDf = dobDateValAddedDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDf = startDateValAddedDf.withColumn("dob", to_date($"dob_temp", "dd-MMM-yyyy")).withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("dob_temp", "start_temp")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf, "dob", year, "18", "74") .select("member_sk","member_id")
   // val ageFilterDf = ageFilterDf_temp.withColumnRenamed("member_sk","membersk") //.withColumnRenamed("member_id","memberid")
   // ageFilterDf.printSchema()
    /*loading ref_hedis table*/
    val refHedisDf = spark.sql(KpiConstants.refHedisLoadQuery)


   /* val valueSet = List("Outpatient")
    val codeSystem = List("CPT","HCPCS","UBREV")*/
    val hedisJoinedForDinominator =  UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"PROCEDURE_CODE","inner",KpiConstants.abaMeasureId,KpiConstants.abavalueSetForDinominator,KpiConstants.abscodeSystemForDinominator)     //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.PROCEDURE_CODE" === $"df3.code","inner").filter($"measureid".===("ABA").&&($"valueset".isin(valueSet:_*).&&($"codesystem".isin(codeSystem:_*)))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem")
    val measurement = UtilFunctions.mesurementYearFilter(hedisJoinedForDinominator,"start_date",year,0,730).select("member_sk").distinct()
    val dinominatorForOutput = ageFilterDf.as("df1").join(measurement.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df1.member_id").distinct()
    val dinominator = ageFilterDf.as("df1").join(measurement.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk").distinct()

    /*dinominator.show(50)*/




    /*val valuesetForDinExcl = List("Pregnancy")
    val codeSytemForExcl = List("ICD%")*/

    val joinForDinominatorExcl = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"PRIMARY_DIAGNOSIS","inner",KpiConstants.abaMeasureId,KpiConstants.abavaluesetForDinExcl,KpiConstants.abacodeSytemForExcl)    //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.PRIMARY_DIAGNOSIS" === $"df3.code","inner").filter($"measureid".===("ABA").&&($"valueset".===("Pregnancy")).&&($"codesystem".like("ICD%"))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem","df3.code")
    val measurementExcl = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl,"start_date",year,0,365)
    val dinominatorExcl = ageFilterDf.as("df1").join(measurementExcl.as("df2"),$"df1.member_sk" === $"df2.member_sk","right_outer").select("df1.member_sk")
    val intersect = dinominator.intersect(dinominatorExcl).select("member_sk").distinct()

    //intersect.show()



    val numeratorValueSet = List("BMI","BMI Percentile")
    val joinForNumerator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"PRIMARY_DIAGNOSIS","inner",KpiConstants.abaMeasureId,KpiConstants.abanumeratorValueSet,KpiConstants.abacodeSytemForExcl)     //dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refHedisDf.as("df3"),$"df2.PRIMARY_DIAGNOSIS" === $"df3.code","inner").filter($"measureid".===("ABA").&&($"valueset".isin(numeratorValueSet:_*)).&&($"codesystem".like("ICD%"))).select("df1.member_sk","df2.start_date_sk","df3.measureid","df3.valueset","df3.codesystem","df3.code")
    /*val startDateValAddedDfForNum = joinForNumerator.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForNum = startDateValAddedDfForNum.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("dob_temp", "start_temp")*/
    val measurementNum = UtilFunctions.mesurementYearFilter(joinForNumerator,"start_date",year,0,365)
    val numeratorDf_temp = ageFilterDf.as("df1").join(dinominator.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk")
    val numeratorDf =   numeratorDf_temp.as("df1").join(measurementNum.as("df3"),$"df1.member_sk" === $"df3.member_sk","right_outer").select("df1.member_sk").distinct()
    val numerator2Df = numeratorDf.intersect(dinominator).select("member_sk").distinct()
   // numerator2Df.show()

    import spark.implicits._

    val lobdetailsData = dinominator.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")
    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,intersect,numerator2Df,dimMemberDf,"ABA")
    formattedOutPutDf.show()

  }
}
