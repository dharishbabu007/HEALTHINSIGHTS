package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
object NcqaOMW {


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

    lookupTableDf = spark.sql(KpiConstants.view45DaysLoadQuery)


    /*common filter checking*/
    val commonFilterDf = joinedFactMembershipDf.as("df1").join(lookupTableDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","left_outer").filter("start_date is null").select("df1.*")

    val dimdateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    val dobDateValAddedDf = commonFilterDf.as("df1").join(dimdateDf.as("df2"),$"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*",$"df2.calendar_date").withColumnRenamed("calendar_date","dob_temp").drop("DATE_OF_BIRTH_SK")
    val dateTypeDf = dobDateValAddedDf.withColumn("dob", to_date($"dob_temp","dd-MMM-yyyy")).drop("dob_temp")





    /*loading ref_hedis table*/
    val refHedisDf = spark.sql(KpiConstants.refHedisLoadQuery)

    /*Age filter*/
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf,"dob",year,"67","85",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal).select("member_sk","gender","dob")
    val genderFilter = ageFilterDf.filter($"gender".===("F")).select("member_sk","dob")



    /*Dinominator Calculation starts*/
    val firstIntakeDate = "01-JUL-"+(year.toInt-1).toString
    val secondIntakeDate = "30-JUN-"+year

    /*Dinominator1 Starts*/
    /*outpatient visit (Outpatient Value Set), an observation visit (Observation Value Set) or an ED visit (ED Value Set)*/
    val hedisJoinedForDinominator1Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.omwMeasureId,KpiConstants.omwOutPatientValueSet,KpiConstants.omwOutPatientCodeSystem)
    val mesurementFilterDf = UtilFunctions.dateBetweenFilter(hedisJoinedForDinominator1Df,"start_date",firstIntakeDate,secondIntakeDate)

    /*fracture (Fractures Value Set) AS Primary Diagnosis*/
    val hedisJoinedForFractureAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.omwMeasureId,KpiConstants.omwFractureValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val mesurementFilterForFractureAsDiagDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsDiagDf,"start_date",firstIntakeDate,secondIntakeDate)


    /*fracture (Fractures Value Set) AS Procedure Code*/
    val hedisJoinedForFractureAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.omwMeasureId,KpiConstants.omwFractureValueSet,KpiConstants.omwFractureCodeSystem)
    val mesurementFilterForFractureAsProDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsProDf,"start_date",firstIntakeDate,secondIntakeDate)

    /*Fracture Dinominator (Union of Fractures Value Set AS Primary Diagnosis and Fractures Value Set AS Procedure Code)*/
    val fractureUnionDf = mesurementFilterForFractureAsDiagDf.union(mesurementFilterForFractureAsProDf)

    /*First Sub Condition of First Dinominator */
    val dinominatorOne_SuboneDf = mesurementFilterDf.intersect(fractureUnionDf)


    /*Second Sub Condition for the First Dinominator*/
    val hedisJoinedForInpatientStDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.omwMeasureId,KpiConstants.omwInpatientStayValueSet,KpiConstants.omwInpatientStayCodeSystem)
    val mesurementFilterForInPatientStayDf = UtilFunctions.mesurementYearFilter(hedisJoinedForInpatientStDf,"start_date",year,184,548).select("member_sk")

    val dinominatorOne_SubTwoDf = mesurementFilterForInPatientStayDf.intersect(fractureUnionDf)


    /*Dinominator1 (union of dinominatorOne_SuboneDf and dinominatorOne_SubTwoDf)*/
    val omwDinominatorOneDf = dinominatorOne_SuboneDf.union(dinominatorOne_SubTwoDf).distinct()
    /*Dinominator1 Ends*/


    /*Dinominator2 Starts*/

    /*Dinominator2 Ends*/


  }
}
