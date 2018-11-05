package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object NcqaCDC10 {

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
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf, "dob", year, "18", "75",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal) .select("member_sk").distinct()
    //ageFilterDf.orderBy("member_sk").show(50)
    /*loading ref_hedis table*/
    val refHedisDf = spark.sql(KpiConstants.refHedisLoadQuery)

    /*calculating Dinominator*/

    /*Dinominator First condition */
    /*val valueSetForDinominatorCdc4 = List("Diabetes")
    val codeSystemForDinominatorCdc4 = List("ICD%")*/
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc4DiabetesvalueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementForFirstDino = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino,"start_date",year,0,730).select("member_sk","start_date")
    val firstDinominatorDf = measurementForFirstDino.select("member_sk")

    /*Dinominator Second Condition*/
    val ref_medvaluesetDf = spark.sql(KpiConstants.refmedvaluesetLoadQuery)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"),$"df2.ndc_number" === $"df3.ndc_code","inner").filter($"measure_id".===("CDC")).select("df1.member_sk","df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino,"start_date",year,0,730).select("member_sk","start_date")
    val secondDinominatorDf = MeasurementForSecondDinoDf.select("member_sk")


    /*union of first and second dinominator condition*/
    val dinominatorUnionDf = firstDinominatorDf.union(secondDinominatorDf).distinct()
    /*dinominator Calculation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk")
    //dinominatorDf.show(50)


    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    val measurementDinominatorExclDf = UtilFunctions.mesurementYearFilter(hospiceDf,"start_date",year,0,365).select("member_sk").distinct()

    /*dinominator Exclusion 2*/
    val hedisJoinedForDiabetesExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdcDiabetesExclValueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDiabetesExclDf,"start_date",year,0,730).select("member_sk").distinct()

    /*Union of Dinominator Exclusion*/
    val dinominatorExclusionDf = measurementDinominatorExclDf.union(measurementDiabetesExclDf).distinct()

    /*Dinominator for kpi calculation*/
    val cdc10DinominatorForKpiCal = dinominatorDf.except(dinominatorExclusionDf)


    /*Numerator Calculation starts*/
    /*Numerator (most recent HbA1c test during the measurement year)*/
    val hedisJoinedForHba1cDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc1NumeratorValueSet,KpiConstants.cdc1NumeratorCodeSystem)
    val measurementForHba1cDf = UtilFunctions.mesurementYearFilter(hedisJoinedForHba1cDf,"start_date",year,0,365).select("member_sk","start_date")
    val numeratorMrecentHba1cDf = UtilFunctions.mostRececntHba1cTest(measurementForHba1cDf,"start_date",year).select("member_sk")



    /*Numerator (HbA1c level is <8.0%)*/
    val hedisJoinedForHba1c8Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc10Hba1cValueSet,KpiConstants.cdc10Hba1cCodeSystem)
    val measurementForHba1c8Df = UtilFunctions.mesurementYearFilter(hedisJoinedForHba1c8Df,"start_date",year,0,365).select("member_sk","start_date").select("member_sk")

    /*Numerator*/
    val Hba1cLessThan8NumeratorDf = numeratorMrecentHba1cDf.intersect(measurementForHba1c8Df).distinct()
    val cdc10NumeratorDf = Hba1cLessThan8NumeratorDf.intersect(cdc10DinominatorForKpiCal)


    /*output format*/
    /*val lobdetailsData = dinominatorDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")

    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,dinominatorExclusionDf,cdc10NumeratorDf,dimMemberDf,KpiConstants.cdcMeasureId)
    formattedOutPutDf.orderBy("MemID").show(100)*/
  }
}
