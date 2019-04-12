package com.itc.ncqa.Functions

import java.sql.Date

import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType}

import scala.util.Try
import scala.collection.JavaConversions._
import collection.mutable._
import scala.collection.mutable


case class Member(member_id:String, service_date:Date)
case class DualMember(member_id:String, lob:String, lob_product:String, payer:String, primary_plan_flag: String)

object UtilFunctions {


  /**
    *
    * @param df (input dataframe)
    * @param headervalues(column names of the input dataframe)
    * @param colName (column name  )
    * @return Dataframe that removes the header if it presents
    */
  def removeHeaderFromDf(df: DataFrame, headervalues: Array[String], colName: String): DataFrame = {

    val df1 = df.filter(df.col(colName).isin(headervalues: _*))
    val returnDf = df.except(df1)
    //println("df and returndf count:"+ df.count()+","+ returnDf.count()+","+ df1.count())
    returnDf
  }

  /**
    *
    * @param df (input Dataframe)
    * @param colName (dob column name which is used to filter the data)
    * @param year (measurement year)
    * @param lower (lower age limit)
    * @param upper (upper age limit)
    * @param lowerIncl (weather include the lower age limit  (Ex:true  or false))
    * @param upperIncl (weather include the upper age limit  (Ex:true or false))
    * @return dataframe that contains the elements satisfy the age filter criteria
    * @usecase function is used to filter the data based on the age criteria
    */
  def ageFilter(df: DataFrame, colName: String, year: String, lower: String, upper: String, lowerIncl: Boolean, upperIncl: Boolean): DataFrame = {
    var current_date = year + "-12-31"

    val inclData = lowerIncl.toString + upperIncl.toString
    //println("inclData value:"+ inclData)
    val newDf1 = df.withColumn("curr_date", lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))
    //newDf2.withColumn("dateDiff", datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25 ).select("member_sk","dateDiff").distinct().show(200)
    val newdf3 = inclData match {

      case "truetrue" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<=(upper.toInt+1))
      case "truefalse" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<(upper.toInt+1))
      case "falsetrue" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<=(upper.toInt+1))
      case "falsefalse" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<(upper.toInt+1))
    }
    newdf3.drop("curr_date")
  }

  /**
    *
    * @param df (input datafarme)
    * @param colName (date column name that is going to use for filtering)
    * @param year(measurement year (31-Dec-year will take as the current date))
    * @param lower (lower period (in days from current date))
    * @param upper (upper period (in days from current date))
    * @return Dataframe that contains the members whoose date column has fallen in the filtering period.
    * @usecase function will filter the elements whoose processed date has fallen in the lower and upper based on the measurement year
    */
  def mesurementYearFilter(df: DataFrame, colName: String, year: String, lower: Int, upper: Int): DataFrame = {


    var current_date = year + "-12-31"
    val df1 = df.withColumn("curr", lit(current_date))
    val newDf = df1.withColumn("dateDiff", datediff(df1.col("curr"), df1.col(colName)))
    val newDf1 = newDf.filter(newDf.col("dateDiff").<=(upper).&&(newDf.col("dateDiff").>=(lower))).drop("curr", "dateDiff")
    newDf1
  }

  /**
    *
    * @param df
    * @param colName
    * @param year
    * @param lower
    * @param upper
    * @return
    */
  def measurementYearFilter(df:DataFrame, colName: String, year: String, lower:Int, upper:Int): DataFrame = {

    var lower_date = year.toInt - lower + "-12-31"
    var upper_date = year.toInt - upper + "-01-01"
    //print("dates--------------------------:"+lower_date +","+upper_date)
    val newDf = df.filter(df.col(colName).>=(lit(upper_date)) && df.col(colName).<=(lit(lower_date)))
    newDf
  }

  /**
    *
    * @param df (input dataframe where the filter has to do)
    * @param colName (date column name)
    * @param date1 (lower date (date should be in "dd-MMM-yyyy"))
    * @param date2 (upper date (date should be in "dd-MMM-yyyy"))
    * @return dataframe that contains the date column between the lower and upper dates
    * @usecase fUnction filter out the data whre the date column fall between the lower and upper dates
    */
  def dateBetweenFilter(df: DataFrame, colName: String, date1: String, date2: String): DataFrame = {

    //print("date1-------------------------------------------------:"+date1)
    val newDf = df.filter(df.col(colName).>=(lit(date1)) && df.col(colName).<=(lit(date2)))
    newDf
  }

  /**
    *
    * @param df(input dataframe)
    * @param colName(start_date column)
    * @param year(measurement year)
    * @return(Dataframe that has the elements who has most recentHBA1C test)
    * @usecase(function filters the elemnts who has most recentHBA1C test)
    */
  def mostRececntHba1cTest(df: DataFrame, colName: String, year: String): DataFrame = {

    var current_date = year + "-12-31"
    val df1 = df.withColumn("curr", lit(current_date))
    val newDf = df1.withColumn("dateDiff", datediff(df1.col("curr"), df1.col(colName)))
    val newDf1 = newDf.groupBy("member_sk").min("dateDiff")
    newDf1
  }

  def getQualityMsrSkFunction(spark:SparkSession,dimQltyMsrDf:DataFrame,dimQltyPgmDf:DataFrame):String = {

    import spark.implicits._

    val df = dimQltyMsrDf.as("df1").join(dimQltyPgmDf.as("df2"),$"df1.quality_program_sk" === $"df2.quality_program_sk", KpiConstants.innerJoinType)
                         .select("df1.quality_measure_sk")
    val dList = df.as[String].collectAsList()
    val qualityMeasureSk = if (dList.isEmpty) "qualitymeasuresk" else dList(0)
    qualityMeasureSk
  }


  /**
    *
    * @param spark(spark session object)
    * @param dimMemberDf(dimMember Dataframe)
    * @param factClaimDf(factClaim Dataframe)
    * @param factMembershipDf(factmembership dataframe)
    * @param dimLocationDf(dimlocation dataframe)
    * @param refLobDf(reflob dataframe)
    * @param facilityDf(facility dataframe)
    * @param lobName (lob name(ex:Commercial,Medicaid,Medicare))
    * @param measureTitle (measure title)
    * @return(Dataframe that contains the columns required for the initial filter and for the output creation)
    * @usecase(function is used to join and get the required columns for initial filter(age filter common filter and gender filter)
    *         and the columns required for the output creation)
    */
  def joinForCommonFilterFunction(spark: SparkSession, dimMemberDf: DataFrame, dimProductPlan: DataFrame, factMembershipDf: DataFrame, dimLocationDf: DataFrame, refLobDf: DataFrame, facilityDf: DataFrame, lobName: String, qltyMsrSk: String): DataFrame = {

    import spark.implicits._

    /*join dim_member,fact_claims,fact_membership and refLob.*/
    val joinedDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType)
                                               /*.join(dimProductPlan.as("df3"), factMembershipDf.col(KpiConstants.productplanSkColName) === dimProductPlan.col(KpiConstants.productplanSkColName), KpiConstants.innerJoinType)
                                               .join(refLobDf.as("df4"), dimProductPlan.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName), KpiConstants.innerJoinType)
                                               .filter(refLobDf.col(KpiConstants.lobColName).===(lobName))*/
                                               .select(s"df1.${KpiConstants.memberskColName}",s"df1.${KpiConstants.dobskColame}", s"df1.${KpiConstants.genderColName}", s"df2.${KpiConstants.productplanSkColName}",s"df2.${KpiConstants.memPlanStartDateSkColName}",s"df2.${KpiConstants.memPlanEndDateSkColName}",
                                               s"df2.${KpiConstants.benefitMedicalColname}")
   /* dimMemberDf.show()
    factMembershipDf.show()*/
    //joinedDf.select(KpiConstants.memberskColName, KpiConstants.dobskColame).show(50)
    /*join the joinedDf with dimLocation for getting facility_sk for doing the further join with dimFacility*/
    //val joinedDimLocationDf = joinedDf.as("df1").join(dimLocationDf.as("df2"), joinedDf.col(KpiConstants.locationSkColName) === dimLocationDf.col(KpiConstants.locationSkColName), KpiConstants.innerJoinType).select("df1.*", "df2.facility_sk")

    /*Join with dimFacility for getting facility_sk*/
   // val facilityJoinedDf = joinedDimLocationDf.as("df1").join(facilityDf.as("df2"), joinedDimLocationDf.col(KpiConstants.facilitySkColName) === facilityDf.col(KpiConstants.facilitySkColName), KpiConstants.innerJoinType).select("df1.member_sk", "df1.date_of_birth_sk", "df1.gender", "df1.lob", "df1.product_plan_sk","df1.member_plan_start_date_sk","df1.member_plan_end_date_sk", "df2.facility_sk")

    /*Load the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*join the data with dim_date for getting calender date of dateofbirthsk,member_plan_start_date_sk,member_plan_end_date_sk*/
    val dobDateValAddedDf = joinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                     .select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp").drop(KpiConstants.dobskColame)

    //dobDateValAddedDf.show(50)
    val memStartDateAddedDf = dobDateValAddedDf.as("df1").join(dimDateDf.as("df2"), dobDateValAddedDf.col(KpiConstants.memPlanStartDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                                .select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_start_temp").drop(KpiConstants.memPlanStartDateSkColName)
    val memEndDateAddedDf = memStartDateAddedDf.as("df1").join(dimDateDf.as("df2"), memStartDateAddedDf.col(KpiConstants.memPlanEndDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                                .select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_end_temp").drop(KpiConstants.memPlanEndDateSkColName)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = memEndDateAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy"))
                                       .withColumn(KpiConstants.memStartDateColName, to_date($"mem_start_temp", "dd-MMM-yyyy"))
                                       .withColumn(KpiConstants.memEndDateColName, to_date($"mem_end_temp", "dd-MMM-yyyy")).drop("dob_temp","mem_start_temp","mem_end_temp")


    /*Adding an additional column(quality_measure_sk) to the calculated df */
    //val finalResultantDf = resultantDf.withColumn(KpiConstants.qualityMsrSkColName, lit(qltyMsrSk))
    resultantDf
  }

  def joinForCommonFilterFunctionForClinical(spark: SparkSession, dimMemberDf: DataFrame, dimPatDf:DataFrame, factImmDf: DataFrame, factMembershipDf: DataFrame, factpatmemDf: DataFrame, dimProductDf:DataFrame, dimLocationDf: DataFrame, refLobDf: DataFrame, facilityDf: DataFrame, lobName: String, measureTitle: String): DataFrame = {

    import spark.implicits._

    /*join dim_pat, fact_pat_mem and dim_member*/
     val patAndMemDf = dimPatDf.as("df1").join(factpatmemDf.as("df2"), dimPatDf.col(KpiConstants.patientidColname) === factpatmemDf.col(KpiConstants.patientidColname), KpiConstants.innerJoinType)
                                                .join(dimMemberDf.as("df3"), factpatmemDf.col(KpiConstants.memberidColName) === dimMemberDf.col(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                .select(s"df1.${KpiConstants.patientSkColname}",s"df1.${KpiConstants.patientidColname}", s"df3.${KpiConstants.memberskColName}",s"df3.${KpiConstants.memberidColName}",s"df3.${KpiConstants.dobskColame}",s"df3.${KpiConstants.genderColName}")


    /*join patAndMemDf,fact_claims,fact_membership and refLob.*/
    val joinedDf = patAndMemDf.as("df1").join(factMembershipDf.as("df2"), patAndMemDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType)
                                               .join(dimProductDf.as("df3"), factMembershipDf.col(KpiConstants.productplanSkColName) === dimProductDf.col(KpiConstants.productplanSkColName), KpiConstants.innerJoinType)
                                               .join(refLobDf.as("df4"), dimProductDf.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName), KpiConstants.innerJoinType).filter(refLobDf.col(KpiConstants.lobColName).===(lobName))
                                               .select("df1.member_sk", "df1.member_id", "df1.patient_sk", "df1.date_of_birth_sk", "df1.gender", "df4.lob","df2.product_plan_sk","df2.member_plan_start_date_sk","df2.member_plan_end_date_sk")


  /*  /*join the joinedDf with dimLocation for getting facility_sk for doing the further join with dimFacility*/
    val joinedDimLocationDf = joinedDf.as("df1").join(dimLocationDf.as("df2"), factClaimDf.col(KpiConstants.locationSkColName) === dimLocationDf.col(KpiConstants.locationSkColName), KpiConstants.innerJoinType).select("df1.*", "df2.facility_sk")

    /*Join with dimFacility for getting facility_sk*/
    val facilityJoinedDf = joinedDimLocationDf.as("df1").join(facilityDf.as("df2"), joinedDimLocationDf.col(KpiConstants.facilitySkColName) === facilityDf.col(KpiConstants.facilitySkColName), KpiConstants.innerJoinType).select("df1.member_sk", "df1.date_of_birth_sk", "df1.gender", "df1.lob", "df1.product_plan_sk","df1.member_plan_start_date_sk","df1.member_plan_end_date_sk", "df2.facility_sk")
*/
    /*Load the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*join the data with dim_date for getting calender date of dateofbirthsk,member_plan_start_date_sk,member_plan_end_date_sk*/
    val dobDateValAddedDf = joinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp").drop(KpiConstants.dobskColame)
    val memStartDateAddedDf = dobDateValAddedDf.as("df1").join(dimDateDf.as("df2"), dobDateValAddedDf.col(KpiConstants.memPlanStartDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_start_temp").drop(KpiConstants.memPlanStartDateSkColName)
    val memEndDateAddedDf = memStartDateAddedDf.as("df1").join(dimDateDf.as("df2"), memStartDateAddedDf.col(KpiConstants.memPlanEndDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "mem_end_temp").drop(KpiConstants.memPlanEndDateSkColName)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = memEndDateAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy")).withColumn(KpiConstants.memStartDateColName, to_date($"mem_start_temp", "dd-MMM-yyyy")).withColumn(KpiConstants.memEndDateColName, to_date($"mem_end_temp", "dd-MMM-yyyy")).drop("dob_temp","mem_start_temp","mem_end_temp")

    /*Finding the quality measure value from quality Measure Table*/
    val qualityMeasureSkList = DataLoadFunctions.dimqualityMeasureLoadFunction(spark, measureTitle).select("quality_measure_sk").as[String].collectAsList()

    val qualityMeasureSk = if (qualityMeasureSkList.isEmpty) "qualitymeasuresk" else qualityMeasureSkList(0)

    /*Adding an additional column(quality_measure_sk) to the calculated df */
    val finalResultantDf = resultantDf.withColumn(KpiConstants.qualityMsrSkColName, lit(qualityMeasureSk))
    finalResultantDf
  }




  def factImmRefHedisJoinFunction(spark: SparkSession, factImmDf: DataFrame, refhedisDf: DataFrame, joinType: String, measureId: String, valueSet: List[String], codeSystem: List[String]): DataFrame = {

    import spark.implicits._

    var newDf = spark.emptyDataFrame


    newDf = factImmDf.join(refhedisDf, factImmDf.col("cvx") === refhedisDf.col("code") || factImmDf.col("cpt") === refhedisDf.col("code") , joinType).filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").isin(codeSystem: _*)))
                     .select(factImmDf.col(KpiConstants.patientSkColname), factImmDf.col(KpiConstants.immStartDateSkColname), factImmDf.col(KpiConstants.expDateSkColName) /*,factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk")*/)


    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val immuneDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.immStartDateSkColname}" === $"df2.${KpiConstants.dateSkColName}").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop(s"${KpiConstants.immStartDateSkColname}")
    val expDateValAddedDf = immuneDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.expDateSkColName}" === $"df2.${KpiConstants.dateSkColName}").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "exp_temp").drop(s"${KpiConstants.expDateSkColName}")

    /*convert the start_date date into dd-MMM-yyy format*/
    val dateTypeDf = expDateValAddedDf.withColumn(KpiConstants.immuneDateColName, to_date($"start_temp", "dd-MMM-yyyy")).withColumn(KpiConstants.expDateColName, to_date($"exp_temp", "dd-MMM-yyyy")).drop( "start_temp","exp_temp")
    dateTypeDf

  }

  def factImmEncRefHedisJoinFunction(spark: SparkSession, factImmDf: DataFrame, factEncDxDf:DataFrame, refhedisDf: DataFrame, joinType: String, measureId: String, valueSet: List[String], codeSystem: List[String]): DataFrame = {

    import spark.implicits._


    val code = codeSystem(0)
    val newDf = factImmDf.as("df1").join(factEncDxDf.as("df2"),factImmDf.col("imm_csn") === factEncDxDf.col("enc_csn_id"), joinType)
                                          .join(refhedisDf.as("df3"), factEncDxDf.col("enc_icd_code") === refhedisDf.col("code"), joinType)
                                          .filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").like(code)))
                                          .select(factImmDf.col(KpiConstants.patientSkColname), factImmDf.col(KpiConstants.immStartDateSkColname), factImmDf.col(KpiConstants.expDateSkColName))

    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val immuneDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.immStartDateSkColname}" === $"df2.${KpiConstants.dateSkColName}").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop(s"${KpiConstants.immStartDateSkColname}")
    val expDateValAddedDf = immuneDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.expDateSkColName}" === $"df2.${KpiConstants.dateSkColName}").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "exp_temp").drop(s"${KpiConstants.expDateSkColName}")

    /*convert the start_date date into dd-MMM-yyy format*/
    val dateTypeDf = expDateValAddedDf.withColumn(KpiConstants.immuneDateColName, to_date($"start_temp", "dd-MMM-yyyy")).withColumn(KpiConstants.expDateColName, to_date($"exp_temp", "dd-MMM-yyyy")).drop( "start_temp","exp_temp")
    dateTypeDf

  }


  /**
    *
    * @param spark(SparkSession object)
    * @param dimMemberDf (dimMember dataframe)
    * @param factClaimDf (factclaim Dataframe)
    * @param refhedisDf(refHedis Dataframe)
    * @param col1(column name with which the factclaim and refHedis jaoin has to do(ex: either "primary_diagnosis" or "procedure_code"))
    * @param joinType (join type (ex:"inner" ,"left_outer","right_outer"))
    * @param measureId (MeasureId for filtering in refhedis df(ex:ABA,CHL etc))
    * @param valueSet (valueset as list for filtering in refhedisdf(ex:List(Pregnancy,BMI) etc))
    * @param codeSystem (codesystem as list for filtering in refhedisdf(ex:List(HCPS,UBREV) etc))
    * @return dataframe that contains the columns required for further filter after the join of 3 tables and the filter based on the condition passed as arguments
    * @usecase Function use to join dimMemberDf,factClaimDf and refhedisDf and filter the data based on the arguments passed as arguments for the initial level of
    *          dinominator ,dinominator exclusion , numerator and numerator exclusion calculation
    */
  def dimMemberFactClaimHedisJoinFunction(spark: SparkSession, dfMap:mutable.Map[String, DataFrame], col1: String, joinType: String, measureId: String, valueSet: List[String], codeSystem: List[String]): DataFrame = {

    import spark.implicits._

    var newDf = spark.emptyDataFrame
    val eligibleDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val factClaimDf = dfMap.get(KpiConstants.factClaimTblName).getOrElse(spark.emptyDataFrame)
    val refhedisDf = dfMap.get(KpiConstants.refHedisTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf = dfMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    /*If measure id is empty dont use the measureid filter*/
    if(KpiConstants.emptyMesureId.equals(measureId)) {

      /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
      if (col1.equalsIgnoreCase("procedure_code")) {
        newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                           .join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
                                            factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), joinType)
                                           .filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").isin(codeSystem: _*)))
                                           .select("df1.*",s"df2.${KpiConstants.serviceDateSkColName}",s"df2.${KpiConstants.providerSkColName}", s"df2.${KpiConstants.admitDateSkColName}", s"df2.${KpiConstants.dischargeDateSkColName}",s"df2.${KpiConstants.claimstatusColName}")
      }
      else {
        val code = codeSystem(0)
        newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                          .join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3") === refhedisDf.col("code") ||
                                          factClaimDf.col("DIAGNOSIS_CODE_4") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7") === refhedisDf.col("code") ||
                                          factClaimDf.col("DIAGNOSIS_CODE_8") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10") === refhedisDf.col("code"), joinType)
                                          .filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").like(code)))
                                          .select("df1.*",s"df2.${KpiConstants.serviceDateSkColName}",s"df2.${KpiConstants.providerSkColName}", s"df2.${KpiConstants.admitDateSkColName}", s"df2.${KpiConstants.dischargeDateSkColName}", s"df2.${KpiConstants.claimstatusColName}")
      }
    }

    else{

      /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
      if (col1.equalsIgnoreCase("procedure_code")) {
        newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                           .join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
                                            factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), joinType)
                                           .filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").isin(codeSystem: _*)))
                                           .select("df1.*",s"df2.${KpiConstants.serviceDateSkColName}",s"df2.${KpiConstants.providerSkColName}", s"df2.${KpiConstants.admitDateSkColName}", s"df2.${KpiConstants.dischargeDateSkColName}", s"df2.${KpiConstants.claimstatusColName}")
      }
      else {
        val code = codeSystem(0)
        newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                           .join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3") === refhedisDf.col("code") ||
                                           factClaimDf.col("DIAGNOSIS_CODE_4") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7") === refhedisDf.col("code") ||
                                           factClaimDf.col("DIAGNOSIS_CODE_8") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10") === refhedisDf.col("code"), joinType)
                                           .filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").like(code)))
                                           .select("df1.*",s"df2.${KpiConstants.serviceDateSkColName}",s"df2.${KpiConstants.providerSkColName}", s"df2.${KpiConstants.admitDateSkColName}", s"df2.${KpiConstants.dischargeDateSkColName}", s"df2.${KpiConstants.claimstatusColName}")
      }
    }




    /*Loading the dim_date table*/


    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val serviceDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.serviceDateSkColName}" === $"df2.${KpiConstants.dateSkColName}",KpiConstants.leftOuterJoinType)
                                   .select($"df1.*", $"df2.${KpiConstants.calenderDateColName}")
                                   .withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.serviceTempColName)
                                   .drop(KpiConstants.serviceDateSkColName)

    val admitDateValAddedDf = serviceDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.admitDateSkColName}" === $"df2.${KpiConstants.dateSkColName}",KpiConstants.leftOuterJoinType)
                                                 .select($"df1.*", $"df2.${KpiConstants.calenderDateColName}")
                                                 .withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.admitTempColName)
                                                 .drop(KpiConstants.admitDateSkColName)

    val dischargeDateValAddedDf = admitDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.dischargeDateSkColName}" === $"df2.${KpiConstants.dateSkColName}",KpiConstants.leftOuterJoinType)
                                                     .select($"df1.*", $"df2.${KpiConstants.calenderDateColName}")
                                                     .withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.dischargeTempColName)
                                                     .drop(KpiConstants.dischargeDateSkColName)


    /*convert the start_date date into dd-MMM-yyy format*/
    val dateTypeDf = dischargeDateValAddedDf.withColumn(KpiConstants.serviceDateColName, to_date($"${KpiConstants.serviceTempColName}", "dd-MMM-yyyy"))
                                            .withColumn(KpiConstants.admitDateColName, to_date($"${KpiConstants.admitTempColName}", "dd-MMM-yyyy"))
                                            .withColumn(KpiConstants.dischargeDateColName, to_date($"${KpiConstants.dischargeTempColName}", "dd-MMM-yyyy"))
                                            .drop( KpiConstants.serviceTempColName,KpiConstants.admitTempColName,KpiConstants.dischargeTempColName)

    dateTypeDf

  }


  def eligibleFactClaimJoinFunction(spark:SparkSession, dfMap:mutable.Map[String, DataFrame]):DataFrame={

    import spark.implicits._

    val eligibleDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val factClaimDf = dfMap.get(KpiConstants.factClaimTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf = dfMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    val newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), Seq(KpiConstants.memberskColName), KpiConstants.innerJoinType)
    newDf
  }

  def dimMemberFactRxclaimsHedisJoinFunction(spark:SparkSession, dfMap:mutable.Map[String, DataFrame],measureId:String, valueSet:List[String]):DataFrame ={

    import spark.implicits._

    val eligibleDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val factRxClaimDf = dfMap.get(KpiConstants.factRxClaimTblName).getOrElse(spark.emptyDataFrame)
    val refMedValSetDf = dfMap.get(KpiConstants.refmedValueSetTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf = dfMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    val joinedDf = eligibleDf.as("df1").join(factRxClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType).join(refMedValSetDf.as("df3"),factRxClaimDf.col(KpiConstants.ndcNmberColName) === refMedValSetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType)
                                              .filter(($"${KpiConstants.medicatiolListColName}".isin(valueSet:_*)) && ($"df3.${KpiConstants.measureIdColName}".===(measureId)))
                                              .select(s"df1.${KpiConstants.memberskColName}", s"df2.${KpiConstants.serviceDateSkColName}")



    val startDateValAddedDf = joinedDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.serviceDateSkColName}" === $"df2.${KpiConstants.dateSkColName}", KpiConstants.leftOuterJoinType)
                                                                    .select(s"df1.${KpiConstants.memberskColName}", s"df2.${KpiConstants.calenderDateColName}")
                                                                    .withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.serviceTempColName)

    val formattedDatecolAddedDf = startDateValAddedDf.withColumn(KpiConstants.rxServiceDateColName, to_date($"${KpiConstants.serviceTempColName}", "dd-MMM-yyyy"))
                                                     .drop(KpiConstants.serviceTempColName)
    formattedDatecolAddedDf

  }

  /**
    *
    * @param spark(SparkSession object)
    * @param factClaimDf (factclaim Dataframe)
    * @param refhedisDf(refHedis Dataframe)
    * @param col1(column name with which the factclaim and refHedis jaoin has to do(ex: either "primary_diagnosis" or "procedure_code"))
    * @param joinType (join type (ex:"inner" ,"left_outer","right_outer"))
    * @param measureId (MeasureId for filtering in refhedis df(ex:ABA,CHL etc))
    * @param valueSet (valueset as list for filtering in refhedisdf(ex:List(Pregnancy,BMI) etc))
    * @param codeSystem (codesystem as list for filtering in refhedisdf(ex:List(HCPS,UBREV) etc))
    * @return dataframe that contains the columns required for further filter after the join of 3 tables and the filter based on the condition passed as arguments
    * @usecase Function use to join dimMemberDf,factClaimDf and refhedisDf and filter the data based on the arguments passed as arguments for the initial level of
    *          dinominator ,dinominator exclusion , numerator and numerator exclusion calculation
    */
  def factClaimRefHedisJoinFunction(spark: SparkSession, factClaimDf: DataFrame, refhedisDf: DataFrame, col1: String, joinType: String, measureId: String, valueSet: List[String], codeSystem: List[String]): DataFrame = {

    import spark.implicits._

    var newDf = spark.emptyDataFrame

    /*If measure id is empty dont use the measureid filter*/
    if(KpiConstants.emptyMesureId.equals(measureId)) {

      /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
      if (col1.equalsIgnoreCase("procedure_code")) {
        newDf = factClaimDf.join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
          factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").isin(codeSystem: _*))).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
      }
      else {
        val code = codeSystem(0)
        newDf = factClaimDf.join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3") === refhedisDf.col("code") ||
          factClaimDf.col("DIAGNOSIS_CODE_4") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7") === refhedisDf.col("code") ||
          factClaimDf.col("DIAGNOSIS_CODE_8") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("valueset").isin(valueSet: _*).&&(refhedisDf.col("codesystem").like(code))).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
        }
      }

    else{

      /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
      if (col1.equalsIgnoreCase("procedure_code")) {
        newDf = factClaimDf.join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
          factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").isin(codeSystem: _*))).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
      }
      else {
        val code = codeSystem(0)
        newDf = factClaimDf.join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3") === refhedisDf.col("code") ||
          factClaimDf.col("DIAGNOSIS_CODE_4") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7") === refhedisDf.col("code") ||
          factClaimDf.col("DIAGNOSIS_CODE_8") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").like(code))).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
      }
    }




    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val startDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val admitDateValAddedDf = startDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.admit_date_sk" === $"df2.date_sk",KpiConstants.leftOuterJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "admit_temp").drop("admit_date_sk")
    val dischargeDateValAddedDf = admitDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.discharge_date_sk" === $"df2.date_sk",KpiConstants.leftOuterJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "discharge_temp").drop("discharge_date_sk")


    /*convert the start_date date into dd-MMM-yyy format*/
    val dateTypeDf = dischargeDateValAddedDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).withColumn("admit_date", to_date($"admit_temp", "dd-MMM-yyyy")).withColumn("discharge_date", to_date($"discharge_temp", "dd-MMM-yyyy")).drop( "start_temp","admit_temp","discharge_temp")
    dateTypeDf

  }

  /**
    *
    * @param spark spark session object
    * @param factRxClaimDf - factrx claim dataframe
    * @param refMedValSetDf - ref medvalueset Dataframe
    * @param measureId measureid for filter in ref_medvaluset
    * @param valueSet - valueset value for filterin ref med valueset
    * @return - Data frame which contains the member sk and start date for the filtering condition
    * @usecase - This function us used to join factrx claim and refmedvalueset and filter based on the value given in the
    *            arguments, is called in the dinominator dinominator ecxclusion and numerator calculation.
    */
  def factRxClaimRefMedValueSetJoinFunction(spark: SparkSession, factRxClaimDf:DataFrame,refMedValSetDf:DataFrame,measureId:String, valueSet:List[String]):DataFrame ={

    import spark.implicits._

    val factRxAndRefmedValJoinedDf = factRxClaimDf.as("df1").join(refMedValSetDf.as("df2"),factRxClaimDf.col(KpiConstants.ndcNmberColName) === refMedValSetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"medication_list".isin(valueSet:_*) && $"measure_id".===(measureId)).select("df1.member_sk", "df1.start_date_sk")

    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    val startDateValAddedDfForThirddDino = factRxAndRefmedValJoinedDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val formattedDatecolAddedDf = startDateValAddedDfForThirddDino.withColumn(KpiConstants.rxStartDateColName, to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    formattedDatecolAddedDf
  }

  /**
    *
    * @param spark (SparkSession Object)
    * @param dimMemberDf (dimmemberDataframe)
    * @param factClaimDf (factclaim Dataframe)
    * @param refhedisDf (refHedis Dataframe)
    * @return Dataframe that contains the memebrs who are in the Hospice condition
    * @usecase function filters the members who are fallen under the hospice condition.
    */
  def hospiceFunction(spark: SparkSession, factClaimDf: DataFrame, refhedisDf: DataFrame): DataFrame = {

    import spark.implicits._

    val hospiceList = List(KpiConstants.hospiceVal)
    // val newDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refhedisDf.as("df3"), $"df2.PROCEDURE_HCPCS_CODE" === "df3.code", "cross").filter($"df3.code".===("G0155")).select("df1.member_sk", "start_date_sk")
    val newDf = factClaimDf.join(refhedisDf, factClaimDf.col("procedure_code") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
      factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), "cross").filter(refhedisDf.col("valueset").isin(hospiceList: _*)).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"))
    val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
    val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    dateTypeDfForDinoExcl.select("member_sk", "start_date")
  }

  /**
    *
    * @param spark
    * @param dfMap
    * @param year
    * @return
    */
  def hospiceExclusionFunction(spark: SparkSession, dfMap:mutable.Map[String, DataFrame], year:String): DataFrame = {

    import spark.implicits._

    val eligibleDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val factClaimDf = dfMap.get(KpiConstants.factClaimTblName).getOrElse(spark.emptyDataFrame)
    val refhedisDf = dfMap.get(KpiConstants.refHedisTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf = dfMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    val endDate = year + "-12-31"
    val startDate = year + "-01-01"
    val hospiceList = List(KpiConstants.hospiceVal)
    val newDf = eligibleDf.as("df1").join(factClaimDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType).join(refhedisDf, factClaimDf.col("procedure_code") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
                                                                                          factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), "cross").filter(refhedisDf.col("valueset").isin(hospiceList: _*)).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"))

    val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.${KpiConstants.serviceDateSkColName}" === $"df2.${KpiConstants.dateSkColName}")
                                                               .select($"df1.*", $"df2.${KpiConstants.calenderDateColName}").withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.serviceTempColName)
                                                               .drop(KpiConstants.serviceDateSkColName)

    val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn(KpiConstants.serviceDateColName, to_date($"${KpiConstants.serviceTempColName}", "dd-MMM-yyyy"))
                                                              .drop(KpiConstants.serviceTempColName)
    dateTypeDfForDinoExcl.filter(($"${KpiConstants.serviceDateColName}".>=(startDate)) && ($"${KpiConstants.serviceDateColName}".<=(endDate)))
                         .select(KpiConstants.memberskColName)
  }

  def hospiceFunctionForClinical(spark: SparkSession, factecDf: DataFrame, factencdxDf:DataFrame, refhedisDf: DataFrame): DataFrame = {

    import spark.implicits._

    val hospiceList = List(KpiConstants.hospiceVal)
    val newDf = factecDf.as("df1").join(factencdxDf.as("df2"),factecDf.col("enc_csn_id") === factencdxDf.col("enc_csn_id"),KpiConstants.innerJoinType)
                                         .join(refhedisDf, factencdxDf.col("enc_icd_code") === refhedisDf.col("code"), KpiConstants.innerJoinType)
                                         .filter(refhedisDf.col("valueset").isin(hospiceList: _*))
                                         .select(factecDf.col(KpiConstants.patientSkColname))
   /* val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
    val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    dateTypeDfForDinoExcl.select("member_sk", "start_date")*/
    newDf
  }

  def hospiceMemberDfFunction(spark: SparkSession, dimMemberDf: DataFrame, factClaimDf: DataFrame, refhedisDf: DataFrame): DataFrame = {

    import spark.implicits._

    val hospiceList = List(KpiConstants.hospiceVal)
   // val newDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refhedisDf.as("df3"), $"df2.PROCEDURE_HCPCS_CODE" === "df3.code", "cross").filter($"df3.code".===("G0155")).select("df1.member_sk", "start_date_sk")
    val newDf = factClaimDf.join(refhedisDf, factClaimDf.col("procedure_code") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
                factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), "cross").filter(refhedisDf.col("valueset").isin(hospiceList: _*)).select(factClaimDf.col("member_sk"), factClaimDf.col("start_date_sk"))
    val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
    val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    dateTypeDfForDinoExcl.select("member_sk", "start_date")
  }

  /**
    *
    * @param spark (SparkSession object)
    * @param dinominatorDf (dinominator Dataframe which used as the super data for create the output data)
    * @param dinoExclDf (dinominator Exclusion Dataframe)
    * @param numeratorDf (Numerator Dataframe)
    * @param numExclDf (Numerator Dataframe)
    * @param outValueList (List of list in which each list contains the Numeartor valuset,DinominatorExclusion valueset,NumeratorExclusion valueset)
    * @param sourceName (source name which calculate from the program argument)
    * @return output formatted Dataframe which contains the columns in the order that present in fact_gaps_in_hedis table in hive
    * @usecase Function is used to create the output format from the dinominator and other df and the resultant dataframe will use to populate to
    *          fact_gaps_in_hedis table in hive.
    */
  def commonOutputDfCreation(spark: SparkSession, dinominatorDf: DataFrame, dinoExclDf: DataFrame, numeratorDf: DataFrame, numExclDf: DataFrame, outValueList: List[List[String]], sourceAndMsrList: List[String]): DataFrame = {


    //var resultantDf = spark.emptyDataFrame

    import spark.implicits._
    val dinoExclList = if(dinoExclDf.count() > 0) dinoExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numList = if (numeratorDf.count() > 0) numeratorDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numExclList = if(numExclDf.count()> 0) numExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numeratorValueSet = outValueList(0)
    val dinoExclValueSet = outValueList(1)
    val numExclValueSet = outValueList(2)


    /*Adding InDinominator,InDinominatorExclusion,Numearator and Numerator Exclusion columns*/
    val inDinoAddedDf = dinominatorDf.withColumn(KpiConstants.outInDinoColName, lit(KpiConstants.yesVal))

    val inDinoExclAddedDf = if (dinoExclList.isEmpty) inDinoAddedDf.withColumn(KpiConstants.outInDinoExclColName, lit(KpiConstants.noVal))
                            else
                            inDinoAddedDf.withColumn(KpiConstants.outInDinoExclColName, when(inDinoAddedDf.col(KpiConstants.memberskColName).isin(dinoExclList: _*), lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))


    val inNumeAddedDf = if(numList.isEmpty) inDinoExclAddedDf.withColumn(KpiConstants.outInNumColName, lit(KpiConstants.noVal))
                        else
                        inDinoExclAddedDf.withColumn(KpiConstants.outInNumColName, when(inDinoExclAddedDf.col(KpiConstants.memberskColName).isin(numList: _*), lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))



    val inNumExclAddedDf = if (numExclList.isEmpty) inNumeAddedDf.withColumn(KpiConstants.outInNumExclColName, lit(KpiConstants.noVal))
                           else
                            inNumeAddedDf.withColumn(KpiConstants.outInNumExclColName, when(inNumeAddedDf.col(KpiConstants.memberskColName).isin(numExclList: _*), lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))



    /*Adding dinominator exception and numerator exception columns*/
    val dinoNumExceptionAddedDf = inNumExclAddedDf.withColumn(KpiConstants.outInDinoExcColName, lit(KpiConstants.noVal)).withColumn(KpiConstants.outInNumExcColName, lit(KpiConstants.noVal))


    /*Adding numerator Reasons columns*/
    val numeratorValueSetColAddedDf = dinoNumExceptionAddedDf.withColumn(KpiConstants.outNu1ReasonColName, when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length - 1 < 0) KpiConstants.emptyStrVal else numeratorValueSet.get(0)))
      .withColumn(KpiConstants.outNu2ReasonColName, when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length - 1 < 1) KpiConstants.emptyStrVal else numeratorValueSet.get(1)))
      .withColumn(KpiConstants.outNu3ReasonColName, when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length - 1 < 2) KpiConstants.emptyStrVal else numeratorValueSet.get(2)))
      .withColumn(KpiConstants.outNu4ReasonColName, when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length - 1 < 3) KpiConstants.emptyStrVal else numeratorValueSet.get(3)))
      .withColumn(KpiConstants.outNu5ReasonColName, when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length - 1 < 4) KpiConstants.emptyStrVal else numeratorValueSet.get(4)))


    /*Adding Dinominator Exclusion Reason columns */
    val dinoExclValueSetColAddedDf = numeratorValueSetColAddedDf.withColumn(KpiConstants.outDinoExcl1ReasonColName, when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length - 1 < 0) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))
      .withColumn(KpiConstants.outDinoExcl2ReasonColName, when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length - 1 < 1) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))
      .withColumn(KpiConstants.outDinoExcl3ReasonColName, when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length - 1 < 2) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))


    /*Adding Numerator Exclusion reason Columns*/
    val numoExclValueSetColAddedDf = dinoExclValueSetColAddedDf.withColumn(KpiConstants.outNumExcl1ReasonColName, when(dinoExclValueSetColAddedDf.col(KpiConstants.outInNumExcColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (numExclValueSet.length - 1 < 0) KpiConstants.emptyStrVal else numExclValueSet.get(0)))
      .withColumn(KpiConstants.outNumExcl2ReasonColName, when(dinoExclValueSetColAddedDf.col(KpiConstants.outInNumExcColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (numExclValueSet.length - 1 < 0) KpiConstants.emptyStrVal else numExclValueSet.get(0)))


    val dateSkVal = DataLoadFunctions.dimDateLoadFunction(spark).filter($"calendar_date".===(date_format(current_date(), "dd-MMM-yyyy"))).select("date_sk").as[String].collectAsList()(0)

    /*Adding Audit columns to the Dataframe*/
    val auditColumnsAddedDf = numoExclValueSetColAddedDf.withColumn(KpiConstants.outCurrFlagColName, lit(KpiConstants.yesVal))
      .withColumn(KpiConstants.outActiveFlagColName, lit(KpiConstants.actFlgVal))
      .withColumn(KpiConstants.outLatestFlagColName, lit(KpiConstants.yesVal))
      .withColumn(KpiConstants.outSourceNameColName, lit(sourceAndMsrList(0)))
      .withColumn(KpiConstants.outUserColName, lit(KpiConstants.userNameVal))
      .withColumn(KpiConstants.outRecCreateDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outRecUpdateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outDateSkColName, lit(dateSkVal))
      .withColumn(KpiConstants.outIngestionDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outMeasureIdColName,lit(sourceAndMsrList(1)))

    /*Adding hedisSk to the Dataframe*/
    val hedisSkColAddedDf = auditColumnsAddedDf.withColumn(KpiConstants.outHedisGapsSkColName, abs(hash(lit(concat(lit(auditColumnsAddedDf.col(KpiConstants.outMemberSkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outProductPlanSkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outQualityMeasureSkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outFacilitySkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outDateSkColName)))))))
    //hedisSkColAddedDf.select(KpiConstants.outHedisGapsSkColName).show()

    /*selecting the outformatted Dataframe in the order of columns*/
    val outColFormattedDf = hedisSkColAddedDf.select(KpiConstants.outFormattedArray.head, KpiConstants.outFormattedArray.tail: _*)
    outColFormattedDf
  }

  /**
    *
    * @param spark (SparkSession Object)
    * @param factMembershipDf(factmembership Dataframe)
    * @param qualityMeasureSk (quality measuresk)
    * @param sourceName (source name which is calculate from the program argument)
    * @return Dataframe that contains the data required to populate in fact_hedis_qms table in hive
    * @usecase function loads the data from fact_gaps_in_hedis table and do the treansformation, add new columns and audit columns
    *          and return in the format that the data to be populated to the fact_hedis_qms table
    */
  def outputCreationForHedisQmsTable(spark: SparkSession, factMembershipDf: DataFrame, qualityMeasureSk: String, sourceName: String): DataFrame = {

    val quality_Msr_Sk = "'" + qualityMeasureSk + "'"

    /*Loading needed data from fact_gaps_in_hedis table*/
    val hedisDataDfLoadQuery = "select " + KpiConstants.memberskColName + "," + KpiConstants.outQualityMeasureSkColName + "," + KpiConstants.outDateSkColName + "," + KpiConstants.outProductPlanSkColName + "," + KpiConstants.outFacilitySkColName + "," + KpiConstants.outInNumColName + "," + KpiConstants.outInDinoColName + "," + KpiConstants.outInDinoExclColName + "," + KpiConstants.outInNumExclColName + "," + KpiConstants.outInDinoExcColName + "," + KpiConstants.outInNumExcColName + " from ncqa_sample.fact_hedis_gaps_in_care where quality_measure_sk =" + quality_Msr_Sk
    val hedisDataDf = spark.sql(hedisDataDfLoadQuery)
    //hedisDataDf.show()
    /*join the hedisDataDf with factmembership data for getting the lob_id */
    val lobIdColAddedDf = hedisDataDf.as("df1").join(factMembershipDf.as("df2"), hedisDataDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.*", "df2.lob_id")
    //lobIdColAddedDf.printSchema()

    /*group by the df using the columns (qualitymeasuresk,datesk,productplansk,facilitysk and lob_id),calculate the required aggregation and add those as new columns*/
    val groupByDf = lobIdColAddedDf.groupBy(KpiConstants.qualityMsrSkColName, KpiConstants.dateSkColName, KpiConstants.outProductPlanSkColName, KpiConstants.facilitySkColName, "lob_id").agg(count(when(lobIdColAddedDf.col(KpiConstants.outInDinoColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsDinominatorColName),
      count(when(lobIdColAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsNumeratorColName),
      count(when(lobIdColAddedDf.col(KpiConstants.outInNumExclColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsNumExclColName),
      count(when(lobIdColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsDinoExclColName),
      count(when(lobIdColAddedDf.col(KpiConstants.outInNumExcColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsNumExcColName),
      count(when(lobIdColAddedDf.col(KpiConstants.outInDinoExcColName).===(KpiConstants.yesVal), true)).alias(KpiConstants.outHedisQmsDinoExcColName))


    /*adding ratio and Bonus columns*/
    val ratioAndBonusColAddedDf = groupByDf.withColumn(KpiConstants.outHedisQmsRatioColName, (groupByDf.col(KpiConstants.outHedisQmsNumeratorColName)./(groupByDf.col(KpiConstants.outHedisQmsDinominatorColName))).*(100)).withColumn(KpiConstants.outHedisQmsBonusColName, lit(0)).withColumn(KpiConstants.outHedisQmsPerformanceColName, lit(0))

    /*Adding the audit columns*/
    val auditColumnsAddedDf = ratioAndBonusColAddedDf.withColumn(KpiConstants.outCurrFlagColName, lit(KpiConstants.yesVal))
      .withColumn(KpiConstants.outActiveFlagColName, lit(KpiConstants.actFlgVal))
      .withColumn(KpiConstants.outLatestFlagColName, lit(KpiConstants.yesVal))
      .withColumn(KpiConstants.outSourceNameColName, lit(sourceName))
      .withColumn(KpiConstants.outUserColName, lit(KpiConstants.userNameVal))
      .withColumn(KpiConstants.outRecCreateDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outRecUpdateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outIngestionDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))


    /*Create and add the qms sk column based on the existing column values*/
    val outHedisQmsSkColAddedDf = auditColumnsAddedDf.withColumn(KpiConstants.outHedisQmsSkColName, abs(hash(lit(concat(lit(auditColumnsAddedDf.col(KpiConstants.outQualityMeasureSkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outProductPlanSkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outHedisQmsLobidColName)), lit(auditColumnsAddedDf.col(KpiConstants.outFacilitySkColName)), lit(auditColumnsAddedDf.col(KpiConstants.outDateSkColName)))))))

    /*selecting the data in the order of columns as same as in the fact_hedis_qms table in hive*/
    val outFormattedDf = outHedisQmsSkColAddedDf.select(KpiConstants.outFactHedisQmsFormattedList.head, KpiConstants.outFactHedisQmsFormattedList.tail: _*)
    //outFormattedDf.printSchema()
    outFormattedDf
  }

  /**
    *
    * @param spark
    * @param inputDf
    * @param startDate
    * @param endDate
    * @param lob_name
    * @return
    */
  def contEnrollAndAllowableGapFilter(spark:SparkSession, inputDf:DataFrame, formatName:String, argMap:Map[String, String]):DataFrame ={

    import spark.implicits._


    val allowableGapDays = KpiConstants.days45

    /*input df creation for Continuos Enrollment and Allowable gap Check based on the format*/
    val contEnrollInDf = formatName match {

      case KpiConstants.commondateformatName => {
                                                val contEnrollStartDate = argMap.get(KpiConstants.dateStartKeyName).getOrElse("")
                                                val contEnrollEndDate = argMap.get(KpiConstants.dateEndKeyName).getOrElse("")
                                                val anchorDate = argMap.get(KpiConstants.dateAnchorKeyName).getOrElse("")

                                        inputDf.withColumn(KpiConstants.contenrollLowCoName, lit(contEnrollStartDate).cast(DateType))
                                               .withColumn(KpiConstants.contenrollUppCoName, lit(contEnrollEndDate).cast(DateType))
                                               .withColumn(KpiConstants.anchorDateColName, lit(anchorDate).cast(DateType))

      }

      case KpiConstants.ageformatName => {
                                              val ageContStart = argMap.get(KpiConstants.ageStartKeyName).getOrElse("")
                                              val ageContEnd = argMap.get(KpiConstants.ageEndKeyName).getOrElse("")
                                              val ageAnchor = argMap.get(KpiConstants.ageAnchorKeyName).getOrElse("")
                                      inputDf.withColumn(KpiConstants.contenrollLowCoName, add_months($"${KpiConstants.dateofbirthColName}", (ageContStart.toInt * KpiConstants.months12)))
                                             .withColumn(KpiConstants.contenrollUppCoName, add_months($"${KpiConstants.dateofbirthColName}", (ageContEnd.toInt * KpiConstants.months12)))
               .withColumn(KpiConstants.anchorDateColName, add_months($"${KpiConstants.dateofbirthColName}", (ageAnchor.toInt * KpiConstants.months12)))

      }


    }

    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      ||($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}") && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}"))))
      .withColumn(KpiConstants.anchorflagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}")), lit(KpiConstants.oneVal.toInt)).otherwise(lit(0)))
      .withColumn(KpiConstants.contEdFlagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}")), lit(1)).otherwise(lit(0)))


    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    val listDf = contEnrollStep1Df.groupBy($"${KpiConstants.memberidColName}")
      .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
        sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName))
      .filter((date_add($"max_mem_end_date",KpiConstants.days45+1).>=($"${KpiConstants.contenrollUppCoName}"))
        && (date_sub($"min_mem_start_date",KpiConstants.days45 +1).<=($"${KpiConstants.contenrollLowCoName}"))
        &&($"${KpiConstants.anchorflagColName}").>(0))
      .select($"${KpiConstants.memberidColName}")

    val contEnrollStep2Df = contEnrollStep1Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select("df1.*")

    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberidColName}").orderBy(org.apache.spark.sql.functions.col(s"${KpiConstants.memEndDateColName}").desc,org.apache.spark.sql.functions.col(s"${KpiConstants.memStartDateColName}"))


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep3Df = contEnrollStep2Df.withColumn(KpiConstants.overlapFlagColName, when(($"${KpiConstants.memStartDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memStartDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))
      && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))))
      ,lit(1))
      .when(($"${KpiConstants.memStartDateColName}".<(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)))
        && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1 ).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal)))
        ,lit(2)).otherwise(lit(0)))
      .withColumn(KpiConstants.coverageDaysColName,when($"${KpiConstants.overlapFlagColName}".===(0) ,datediff(when($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"), $"${KpiConstants.memEndDateColName}").otherwise($"${KpiConstants.contenrollUppCoName}")
        ,when($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}"), $"${KpiConstants.memStartDateColName}").otherwise($"${KpiConstants.contenrollLowCoName}"))+ 1 )
        .when($"${KpiConstants.overlapFlagColName}".===(2), datediff( when($"${KpiConstants.contenrollLowCoName}".<=(lag( $"${KpiConstants.contenrollUppCoName}",1).over(contWindowVal)), $"${KpiConstants.memEndDateColName}").otherwise(lag( $"${KpiConstants.contenrollUppCoName}",1).over(contWindowVal))
          ,$"${KpiConstants.memStartDateColName}")+1 )
        .otherwise(0))
      .withColumn(KpiConstants.countColName, when(when($"${KpiConstants.overlapFlagColName}".===(0), datediff(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
        .otherwise(0).>(1),lit(1))
        .otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep3Df.groupBy(KpiConstants.memberidColName)
      .agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
        sum($"${KpiConstants.coverageDaysColName}").alias(KpiConstants.coverageDaysColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))



    val contEnrollmemDf = contEnrollStep5Df.filter(((($"${KpiConstants.countColName}") + (when(date_sub($"min_mem_start_date", 1).>($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
      + (when(date_add($"max_mem_end_date", 1).<($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
      && ($"${KpiConstants.coverageDaysColName}".>=(320)))
      .select(KpiConstants.memberidColName).distinct()



    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}")

    contEnrollDf

  }

  def getVisits(member:String,dates:Array[Long]):(String,Int) ={
    var previous_item =dates(0)
    var visits=1
    for (item<-dates)
    {
      if (item-previous_item < 1209600000 ) {

      }
      else if(item-previous_item >= 1209600000) {
        previous_item=item
        visits = visits+1
      }
    }
    (member,visits)
  }

  def getMembers(member:String,dates:Array[Long]):(String,String) ={


    if(dates(dates.length -1)- dates(0)>= 12614400000L){
      //print("first loop")
      return (member, "Y")
    }
    else if(dates.length == 2){
      //print("Second loop")
      return (member, "N")
    }
    else {
      var a, counter = 0
      var b = 1
      // print("Third loop")
      while (b < dates.length){

        val diff = dates(b) - dates(a)
        if (diff >= 1209600000) {
          counter += 1
          if (counter >= 2) {
            return (member, "Y")
          }
          a = b
          b = b+1
        }
        else {
          b = b + 1
        }
      }
      return (member, "N")
    }
  }

  def imaHpvNumeratorCalculation(spark: SparkSession,dfMap:DataFrame):DataFrame ={


    import spark.implicits._

    /*Numerator3 Calculation (IMAHPV screening or monitoring test)*/
    val imaHpvValueSet = List(KpiConstants.hpvVal)
    val imaHpvCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imaHpvAgeFilterDf = dfMap.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hpvVal))
      && ($"${KpiConstants.serviceDateColName}".>=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months108)))
      && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months156))))
      .select(s"${KpiConstants.memberidColName}", s"${KpiConstants.serviceDateColName}")

    //imaHpvAgeFilterDf.show()

    val atleast2HPVoccurMemDf = imaHpvAgeFilterDf.groupBy(KpiConstants.memberidColName).agg(count(KpiConstants.serviceDateColName).alias("count"))
      .filter($"count".>=(2))
      .select(KpiConstants.memberidColName).rdd
      .map(r=> r.getString(0))
      .collect()

    val imaHpvInDf = imaHpvAgeFilterDf.filter($"${KpiConstants.memberidColName}".isin(atleast2HPVoccurMemDf:_*))

    val hpvInDs = imaHpvInDf.as[Member]

    val groupedDs = hpvInDs.groupByKey(inDs => (inDs.member_id))
      .mapGroups((k,itr) => (k,itr.map(f=> f.service_date.getTime).toArray.sorted))


    val imaHpvDf = groupedDs.map(f=> UtilFunctions.getMembers(f._1,f._2))
      .filter(f=> f._2.equals("Y")).select("_1").toDF("member_id")

    imaHpvDf
  }

  /**
    *
    * @param spark
    * @param dfMap
    * @return
    */
  def initialJoinFunction(spark:SparkSession, dfMap:mutable.Map[String, DataFrame]):DataFrame ={


    import spark.implicits._


    val membershipDf = dfMap.get(KpiConstants.membershipTblName).getOrElse(spark.emptyDataFrame)
    val visitDf = dfMap.get(KpiConstants.visitTblName).getOrElse(spark.emptyDataFrame)

    val resultantDf = membershipDf.as("df1").join(visitDf.as("df2"),Seq(s"${KpiConstants.memberidColName}"), KpiConstants.leftOuterJoinType)

    resultantDf
  }

  def joinWithRefHedisFunction(spark:SparkSession, dfMap:mutable.Map[String,DataFrame], valueset:List[String],medList: List[String]):DataFrame ={

    import spark.implicits._

    val factClaimDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val refhedisDf = dfMap.get(KpiConstants.refHedisTblName).getOrElse(spark.emptyDataFrame)
    val refmedDf = dfMap.get(KpiConstants.refmedValueSetTblName).getOrElse(spark.emptyDataFrame)

    var medListDf = spark.emptyDataFrame
    var listDf = spark.emptyDataFrame



    //<editor-fold desc=" Ref_Hedis table Join Function">

    for(value <- valueset) {

      val codesystemList = refhedisDf.filter($"${KpiConstants.valuesetColName}".===(value))
        .select(KpiConstants.codesystemColname)
        .distinct()
        .rdd
        .map(r=> r.getString(0))
        .collect()

      var inDf = spark.emptyDataFrame

      for (item <- codesystemList) {

        val df = item match {

          case KpiConstants.modifierCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.proccodemod1ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.proccodemod2ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.proccode2mod1ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.proccode2mod2ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.modifierCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")


          case KpiConstants.cptCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), ($"df1.${KpiConstants.proccodeColName}" === $"df2.${KpiConstants.codeColName}"), KpiConstants.innerJoinType)
            .filter($"df2.${KpiConstants.valuesetColName}".===(value)
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.cptCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.cptCatIIVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.proccode2ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter($"df2.${KpiConstants.valuesetColName}".===(value)
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.cptCatIIVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.cvxCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.medcodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.cvxCodeVal))
              &&($"df1.${KpiConstants.medcodeflagColName}".===("C")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.hcpsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.prochcpscodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.hcpsCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.icd9cmCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.primaryDiagnosisColname}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode2ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode3ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode4ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode5ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode6ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode7ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode8ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode9ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode10ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.icd9cmCodeVal))
              &&($"df1.${KpiConstants.icdflagColName}".===("9")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")


          case KpiConstants.icd10cmCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.primaryDiagnosisColname}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode2ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode3ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode4ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode5ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode6ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode7ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode8ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode9ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.diagcode10ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.icd10cmCodeVal))
              &&($"df1.${KpiConstants.icdflagColName}".===("X")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.icd9pcsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.priicdprocColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc2ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc3ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc4ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc5ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc6ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.icd9pcsCodeVal))
              && ($"df1.${KpiConstants.icdflagColName}".===("9")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.icd10pcsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.priicdprocColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc2ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc3ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc4ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc5ColName}" === $"df2.${KpiConstants.codeColName}"
            || $"df1.${KpiConstants.icdproc6ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.icd10pcsCodeVal))
              && ($"df1.${KpiConstants.icdflagColName}".===("X")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")


          case KpiConstants.loincCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), ($"df1.${KpiConstants.loinccodeColName}" === $"df2.${KpiConstants.codeColName}"), KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.loincCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")


          case KpiConstants.posCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.poscodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.posCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.rxnormCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.medcodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.rxnormCodeVal))
              &&($"df1.${KpiConstants.medcodeflagColName}".===("R")))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.snomedctCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.snomedColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.snomedctCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.ubrevCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.revenuecodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              && ($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.ubrevCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

          case KpiConstants.ubtobCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.billtypecodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
            .filter(($"df2.${KpiConstants.valuesetColName}".===(value))
              &&($"df2.${KpiConstants.codesystemColname}".===(KpiConstants.ubtobCodeVal)))
            .select("df1.*", s"df2.${KpiConstants.valuesetColName}")

        }

        if (inDf == spark.emptyDataFrame) {

          inDf = df
        }
        else {

          inDf = inDf.union(df)
        }

      }

      if(inDf != spark.emptyDataFrame) {
        if (listDf == spark.emptyDataFrame) {

          listDf = inDf
        }
        else {

          listDf = listDf.union(inDf)
        }
      }
    }
    //</editor-fold>

    //<editor-fold desc="Ref_Med_Value_Set table Join Function">

    if(!medList.isEmpty) {

      for (value <- medList) {

        var medInDf = spark.emptyDataFrame

        medInDf = factClaimDf.as("df1").join(refmedDf.as("df2"), $"df1.${KpiConstants.ndcCodeColName}" === $"df2.${KpiConstants.ndcCodeColName}", KpiConstants.innerJoinType)
          .filter($"df2.${KpiConstants.medicatiolListColName}".===(value))
          .select("df1.*", s"df2.${KpiConstants.medicatiolListColName}")

        if (medInDf != spark.emptyDataFrame) {
          if (medListDf == spark.emptyDataFrame) {
            medListDf = medInDf
          }
          else {
            medListDf = medListDf.union(medInDf)
          }
        }
      }
      if(medListDf != spark.emptyDataFrame){
        medListDf = medListDf.withColumnRenamed(KpiConstants.medicatiolListColName, KpiConstants.valuesetColName)
      }
    }
    //</editor-fold>

    if(medListDf != spark.emptyDataFrame){

      listDf = listDf.union(medListDf)
    }

    listDf
  }

  /**
    *
    * @param spark
    * @param dfMap
    * @param valueset
    * @return
    */
  def joinWithRefMedFunction(spark:SparkSession, dfMap:mutable.Map[String,DataFrame], valueset:List[String]):DataFrame ={


    import spark.implicits._

    val factClaimDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val refmedDf = dfMap.get(KpiConstants.refmedValueSetTblName).getOrElse(spark.emptyDataFrame)


    val resultantTmpDf = factClaimDf.as("df1").join(refmedDf.as("df2"), $"df1.${KpiConstants.ndcCodeColName}" === $"df2.${KpiConstants.ndcCodeColName}" , KpiConstants.innerJoinType)
                                                  .filter($"df2.${KpiConstants.medicatiolListColName}".isin(valueset:_*))
                                                  .select("df1.*",s"df2.${KpiConstants.medicatiolListColName}")

    val resultantDf = resultantTmpDf.withColumnRenamed(KpiConstants.medicatiolListColName,KpiConstants.valuesetColName)
    resultantDf
  }

  /**
    *
    * @param spark
    * @param dfMap - Map that contains all the required Dataframes
    * @param measureId - Measureid value for which the o/p format is creating
    * @return - Dataframe in the NCQA o/p format
    */
  def ncqaOutputDfCreation(spark: SparkSession, dfMap:mutable.Map[String, DataFrame],measureId: String): DataFrame = {


    import spark.implicits._

    val totalPopDf = dfMap.get(KpiConstants.totalPopDfName).getOrElse(spark.emptyDataFrame)
    val eligiblePopDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val mandatoryExclDf = dfMap.get(KpiConstants.mandatoryExclDfname).getOrElse(spark.emptyDataFrame)
    val optionalExclDf = dfMap.get(KpiConstants.optionalExclDfName).getOrElse(spark.emptyDataFrame)
    val numeratorPopDf = dfMap.get(KpiConstants.numeratorDfName).getOrElse(spark.emptyDataFrame)



    val eligiblePopList = if(eligiblePopDf.count() > 0) eligiblePopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val mandatoryExclList = if(mandatoryExclDf.count() > 0) mandatoryExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val optionalExclList = if(optionalExclDf.count() > 0) optionalExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numeratorPopList = if(numeratorPopDf.count() > 0) numeratorPopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))




    val epopColAddedDf = if (eligiblePopList.isEmpty) totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, lit(KpiConstants.zeroVal))
                         else
                             totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, when(totalPopDf.col(KpiConstants.ncqaOutmemberIdCol).isin(eligiblePopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))



    val exclColAddedDf = if (optionalExclList.isEmpty) epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, lit(KpiConstants.zeroVal))
                         else
                             epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, when(epopColAddedDf.col(KpiConstants.ncqaOutmemberIdCol).isin(optionalExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val rexclColAddedDf = if (mandatoryExclList.isEmpty) exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, lit(KpiConstants.zeroVal))
                          else
                              exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, when(exclColAddedDf.col(KpiConstants.ncqaOutmemberIdCol).isin(mandatoryExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val numColAddedDf = if (numeratorPopList.isEmpty) rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, lit(KpiConstants.zeroVal))
                        else
                            rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, when(exclColAddedDf.col(KpiConstants.ncqaOutmemberIdCol).isin(numeratorPopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val measAddedDf = numColAddedDf.withColumn(KpiConstants.ncqaOutIndCol, lit(KpiConstants.zeroVal))

    val resultDf = measAddedDf.select(KpiConstants.outncqaFormattedList.head, KpiConstants.outncqaFormattedList.tail: _*)
    resultDf

  }

  /**
    *
    * @param dualMem1
    * @param dualMem2
    * @return
    */
  def memberSelection(dualMem1:DualMember, dualMem2:DualMember):DualMember ={


    val lobAddedString = dualMem1.lob + dualMem2.lob
    println( "data is :"+ lobAddedString)

    val resultMember = lobAddedString match {


      case "CommercialMedicaid" =>  dualMem1

      case "MedicaidCommercial"=>  dualMem2

      case "MedicaidMedicare" => dualMem2

      case "MedicareMedicaid" => dualMem1

      case "MedicaidMedicaid" => if(dualMem1.primary_plan_flag.equalsIgnoreCase(KpiConstants.yesVal)){dualMem1} else {dualMem2}

      case "CommercialMedicare"  => if(dualMem1.primary_plan_flag.equalsIgnoreCase(KpiConstants.yesVal)){dualMem1}
      else if(dualMem2.primary_plan_flag.equalsIgnoreCase(KpiConstants.yesVal)){dualMem2}
      else {dualMem2}

      case "MedicareCommercial"  => if(dualMem1.primary_plan_flag.equalsIgnoreCase(KpiConstants.yesVal)){dualMem1}
      else if(dualMem2.primary_plan_flag.equalsIgnoreCase(KpiConstants.yesVal)){dualMem2}
      else {dualMem1}

      case "CommercialCommercial" => dualMem1
    }
    resultMember
  }

  def baseOutDataframeCreation(spark:SparkSession, dataframe:DataFrame, validLobList:List[String],measureId:String):DataFrame ={


    import spark.implicits._

    val outSchema = dataframe.schema

    val outDf = spark.emptyDataFrame


    val medicaidList = List(KpiConstants.mdPayerVal, KpiConstants.mliPayerVal, KpiConstants.mrbPayerVal)


    //<editor-fold desc="Dual Eligibility Logic">

    val dualEligibilityList = List(KpiConstants.sn1PayerVal, KpiConstants.sn2PayerVal, KpiConstants.sn3PayerVal,KpiConstants.mdePayerVal)
    val snPayerList = List(KpiConstants.sn1PayerVal, KpiConstants.sn2PayerVal, KpiConstants.sn3PayerVal)
    val dualEligibleMemInDf = dataframe.filter($"${KpiConstants.payerColName}".isin(dualEligibilityList:_*))
    val mdeMemDf = dataframe.filter($"${KpiConstants.payerColName}".===(KpiConstants.mdePayerVal))
    val dualEligibleMemIdDf = dualEligibleMemInDf.select(KpiConstants.memberidColName).distinct()

    val mcdAddedDualEligMemDf = dualEligibleMemInDf.filter($"${KpiConstants.payerColName}".===(KpiConstants.mdePayerVal))
                                                   .withColumn(KpiConstants.lobColName, lit(KpiConstants.medicaidLobName))
                                                   .withColumn(KpiConstants.payerColName, lit(KpiConstants.mcdPayerVal))



    val mcrAddedDualEligMemDf = dualEligibleMemInDf.filter($"${KpiConstants.payerColName}".isin(snPayerList:_*))
                                                   .withColumn(KpiConstants.lobColName, lit(KpiConstants.medicareLobName))
                                                   .withColumn(KpiConstants.payerColName, lit(KpiConstants.mcrPayerVal))

    val inDf = dataframe.except(mdeMemDf)
    val dualEligAddedResDf = inDf.union(mcdAddedDualEligMemDf).union(mcrAddedDualEligMemDf)

    val dualEligibleMemDf = dualEligAddedResDf.as("df1").join(dualEligibleMemIdDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")

    val dualEligRemMemDf = dualEligAddedResDf.except(dualEligibleMemDf)
    //</editor-fold>

    //<editor-fold desc="Commercial with Different Payer Logic">

    val comercialEnrollMemDf = dualEligRemMemDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.commercialLobName))
      .groupBy(KpiConstants.memberidColName).agg(count(KpiConstants.payerColName).alias("count1"),
      countDistinct(KpiConstants.payerColName).alias("count2"))
      .filter($"count1".===($"count2") && $"count1".>(1))
      .select(KpiConstants.memberidColName)


    val distCommEnrollDf = dualEligRemMemDf.as("df1").join(comercialEnrollMemDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")

    val distPayerCommRemMemDf = dualEligRemMemDf.as("df1").join(comercialEnrollMemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
      .filter($"df2.${KpiConstants.memberidColName}".isNull)
      .select("df1.*")
    //</editor-fold>

    //<editor-fold desc="Single Enrolled Data Logic">

    val singleEnrolledMemIdDf = distPayerCommRemMemDf.groupBy($"${KpiConstants.memberidColName}").agg(count(KpiConstants.lobColName).alias("count"))
      .filter($"count".===(1))
      .select(KpiConstants.memberidColName)

    val singleEnrollMemDf = distPayerCommRemMemDf.as("df1").join(singleEnrolledMemIdDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")
    //singleEnrollMemDf.show()
    //</editor-fold>

    //<editor-fold desc="Dual enrolled Data Logic">

    val multipleEnrollMemDf = distPayerCommRemMemDf.except(singleEnrollMemDf)
    val multipleEnrolledMemInDf = multipleEnrollMemDf.select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName,
                                                             KpiConstants.payerColName, KpiConstants.primaryPlanFlagColName)
    val multipleEnrollDs = multipleEnrolledMemInDf.as[DualMember]

    val dualEnrolledDataDf = multipleEnrollDs.groupByKey(inDs => (inDs.member_id)).reduceGroups(memberSelection(_,_))
      .map(f=> f._2).toDF()

   // dualEnrolledDataDf.show()

    val dualEnrollOutDf = multipleEnrollMemDf.as("df1").join(dualEnrolledDataDf.as("df2"), $"df1.${KpiConstants.memberidColName}"=== $"df2.${KpiConstants.memberidColName}" && $"df1.${KpiConstants.payerColName}"=== $"df2.${KpiConstants.payerColName}", KpiConstants.innerJoinType)
                                             .select("df1.*")

    //</editor-fold>

    val dualAndSingleEnrolledDf = singleEnrollMemDf.union(dualEnrollOutDf).union(distCommEnrollDf).union(dualEligibleMemDf)

    //<editor-fold desc=" Mmp Logic">

    var mmpLobDf = spark.emptyDataFrame

    if(validLobList.contains(KpiConstants.medicareLobName) && validLobList.contains(KpiConstants.medicaidLobName)) {

      val mmpMedicareDf = dualAndSingleEnrolledDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName))
                                                 .withColumn(KpiConstants.lobColName, lit(KpiConstants.medicareLobName))
                                                 .withColumn(KpiConstants.payerColName,lit(KpiConstants.mcrPayerVal))

      val mmpMedicaidDf = dualAndSingleEnrolledDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName))
                                                 .withColumn(KpiConstants.lobColName, lit(KpiConstants.medicaidLobName))
                                                 .withColumn(KpiConstants.payerColName, lit(KpiConstants.mcdPayerVal))


      mmpLobDf = dualAndSingleEnrolledDf.union(mmpMedicareDf).union(mmpMedicaidDf)
    }
    else if(validLobList.contains(KpiConstants.medicareLobName) &&  !(validLobList.contains(KpiConstants.medicaidLobName))){

      mmpLobDf = dualAndSingleEnrolledDf.withColumn(KpiConstants.payerColName, when($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName), lit(KpiConstants.mcrPayerVal)).otherwise($"${KpiConstants.payerColName}"))
                                        .withColumn(KpiConstants.lobColName, when($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName), lit(KpiConstants.medicareLobName)).otherwise($"${KpiConstants.lobColName}"))

    }

    else if(validLobList.contains(KpiConstants.medicaidLobName) &&  !(validLobList.contains(KpiConstants.medicareLobName))){

      mmpLobDf = dualAndSingleEnrolledDf.withColumn(KpiConstants.payerColName, when($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName), lit(KpiConstants.mcdPayerVal)).otherwise($"${KpiConstants.payerColName}"))
                                        .withColumn(KpiConstants.lobColName, when($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName), lit(KpiConstants.medicaidLobName)).otherwise($"${KpiConstants.lobColName}"))

    }
    else{
      mmpLobDf = dualAndSingleEnrolledDf
    }
    //</editor-fold>

    //<editor-fold desc="MarketPlace Logic">

    var marketPlaceLobAddedDf = spark.emptyDataFrame

    if(!validLobList.contains(KpiConstants.commercialLobName)) {

      marketPlaceLobAddedDf =  mmpLobDf.except(mmpLobDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.marketplaceLobName)))

    }
    else {

      marketPlaceLobAddedDf =  mmpLobDf

    }
    //</editor-fold>

    val resultDf = marketPlaceLobAddedDf.withColumn(KpiConstants.payerColName, when($"${KpiConstants.payerColName}".isin(medicaidList:_*),lit(KpiConstants.mcdPayerVal)).otherwise($"${KpiConstants.payerColName}"))

    resultDf
  }

  def toutOutputCreation(spark:SparkSession, dataframe:DataFrame,msrlist:List[String], lobName:List[String], removMsrList:List[String]):DataFrame={

    import spark.implicits._

    val allMsrDf = dataframe.filter($"${KpiConstants.lobColName}".isin(lobName:_*))

    val restMsrDf = dataframe.except(allMsrDf)

    //<editor-fold desc="All Measures Added df">

    var allMsrMeasAddedDf = spark.emptyDataFrame
    for(value <- msrlist){
      val df = allMsrDf.withColumn(KpiConstants.ncqaOutMeasureCol, lit(value))
      if(allMsrMeasAddedDf == spark.emptyDataFrame){
        allMsrMeasAddedDf = df
      }
      else {
        allMsrMeasAddedDf = allMsrMeasAddedDf.union(df)
      }
    }
    //</editor-fold>

    //<editor-fold desc="Restricted Msr Added df">

    val restMsrList = msrlist diff(removMsrList)
    var restMsrMeasAddedDf = spark.emptyDataFrame
    for(value <- restMsrList){
      val df = restMsrDf.withColumn(KpiConstants.ncqaOutMeasureCol, lit(value))
      if(restMsrMeasAddedDf == spark.emptyDataFrame){
        restMsrMeasAddedDf = df
      }
      else {
        restMsrMeasAddedDf = restMsrMeasAddedDf.union(df)
      }
    }
    //</editor-fold>

    val resultDf = allMsrMeasAddedDf.union(restMsrMeasAddedDf)
                                    .withColumnRenamed(KpiConstants.payerColName, KpiConstants.ncqaOutPayerCol)
    resultDf
  }

  def ncqaOutputDfCreation1(spark: SparkSession, dfMap:mutable.Map[String, DataFrame]): DataFrame = {


    import spark.implicits._

    val totalPopDf = dfMap.get(KpiConstants.totalPopDfName).getOrElse(spark.emptyDataFrame)
    val eligiblePopDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val mandatoryExclDf = dfMap.get(KpiConstants.mandatoryExclDfname).getOrElse(spark.emptyDataFrame)
    val optionalExclDf = dfMap.get(KpiConstants.optionalExclDfName).getOrElse(spark.emptyDataFrame)
    val numeratorPopDf = dfMap.get(KpiConstants.numeratorDfName).getOrElse(spark.emptyDataFrame)
    val numerator2PopDf = dfMap.get(KpiConstants.numerator2DfName).getOrElse(spark.emptyDataFrame)
    val numerator3PopDf = dfMap.get(KpiConstants.numerator3DfName).getOrElse(spark.emptyDataFrame)
    val numerator4PopDf = dfMap.get(KpiConstants.numerator4DfName).getOrElse(spark.emptyDataFrame)
    val numerator5PopDf = dfMap.get(KpiConstants.numerator5DfName).getOrElse(spark.emptyDataFrame)



    val eligiblePopList = if(eligiblePopDf.count() > 0) eligiblePopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val mandatoryExclList = if(mandatoryExclDf.count() > 0) mandatoryExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val optionalExclList = if(optionalExclDf.count() > 0) optionalExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numeratorPopList = if(numeratorPopDf.count() > 0) numeratorPopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator2PopList = if(numerator2PopDf.count() > 0) numerator2PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator3PopList = if(numerator3PopDf.count() > 0) numerator3PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator4PopList = if(numerator4PopDf.count() > 0) numerator4PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator5PopList = if(numerator5PopDf.count() > 0) numerator5PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))




    val epopColAddedDf = if (eligiblePopList.isEmpty) totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, lit(KpiConstants.zeroVal))
    else
      totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, when(totalPopDf.col(KpiConstants.memberidColName).isin(eligiblePopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))



    val exclColAddedDf = if (optionalExclList.isEmpty) epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, lit(KpiConstants.zeroVal))
    else
      epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, when(epopColAddedDf.col(KpiConstants.memberidColName).isin(optionalExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val rexclColAddedDf = if (mandatoryExclList.isEmpty) exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, lit(KpiConstants.zeroVal))
    else
      exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, when(exclColAddedDf.col(KpiConstants.memberidColName).isin(mandatoryExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val numColAddedDf = rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, lit(KpiConstants.zeroVal))
                                       .withColumn(KpiConstants.ncqaOutNumCol, when(($"${KpiConstants.memberidColName}".isin(numeratorPopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.imamenMeasureId), lit(KpiConstants.oneVal))
                                                                              .when(($"${KpiConstants.memberidColName}".isin(numerator2PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.imatdMeasureId), lit(KpiConstants.oneVal))
                                                                              .when(($"${KpiConstants.memberidColName}".isin(numerator3PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.imahpvMeasureId), lit(KpiConstants.oneVal))
                                                                              .when(($"${KpiConstants.memberidColName}".isin(numerator4PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.imacmb1MeasureId), lit(KpiConstants.oneVal))
                                                                              .when(($"${KpiConstants.memberidColName}".isin(numerator5PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.imacmb2MeasureId), lit(KpiConstants.oneVal))
                                                                              .otherwise($"${KpiConstants.ncqaOutNumCol}"))



    val measAddedDf = numColAddedDf.withColumnRenamed(KpiConstants.memberidColName, KpiConstants.ncqaOutmemberIdCol)
                                   .withColumn(KpiConstants.ncqaOutIndCol, lit(KpiConstants.zeroVal))

    val resultDf = measAddedDf.select(KpiConstants.outncqaFormattedList.head, KpiConstants.outncqaFormattedList.tail: _*)
    resultDf

  }

  def ncqaW15OutputDfCreation(spark: SparkSession, dfMap:mutable.Map[String, DataFrame]): DataFrame = {


    import spark.implicits._

    val totalPopDf = dfMap.get(KpiConstants.totalPopDfName).getOrElse(spark.emptyDataFrame)
    val eligiblePopDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val mandatoryExclDf = dfMap.get(KpiConstants.mandatoryExclDfname).getOrElse(spark.emptyDataFrame)
    val optionalExclDf = dfMap.get(KpiConstants.optionalExclDfName).getOrElse(spark.emptyDataFrame)
    val numeratorPopDf = dfMap.get(KpiConstants.numeratorDfName).getOrElse(spark.emptyDataFrame)
    val numerator2PopDf = dfMap.get(KpiConstants.numerator2DfName).getOrElse(spark.emptyDataFrame)
    val numerator3PopDf = dfMap.get(KpiConstants.numerator3DfName).getOrElse(spark.emptyDataFrame)
    val numerator4PopDf = dfMap.get(KpiConstants.numerator4DfName).getOrElse(spark.emptyDataFrame)
    val numerator5PopDf = dfMap.get(KpiConstants.numerator5DfName).getOrElse(spark.emptyDataFrame)
    val numerator6PopDf = dfMap.get(KpiConstants.numerator6DfName).getOrElse(spark.emptyDataFrame)
    val numerator7PopDf = dfMap.get(KpiConstants.numerator7DfName).getOrElse(spark.emptyDataFrame)



    val eligiblePopList = if(eligiblePopDf.count() > 0) eligiblePopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val mandatoryExclList = if(mandatoryExclDf.count() > 0) mandatoryExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val optionalExclList = if(optionalExclDf.count() > 0) optionalExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numeratorPopList = if(numeratorPopDf.count() > 0) numeratorPopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator2PopList = if(numerator2PopDf.count() > 0) numerator2PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator3PopList = if(numerator3PopDf.count() > 0) numerator3PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator4PopList = if(numerator4PopDf.count() > 0) numerator4PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator5PopList = if(numerator5PopDf.count() > 0) numerator5PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator6PopList = if(numerator6PopDf.count() > 0) numerator6PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator7PopList = if(numerator7PopDf.count() > 0) numerator7PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))




    val epopColAddedDf = if (eligiblePopList.isEmpty) totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, lit(KpiConstants.zeroVal))
    else
      totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, when(totalPopDf.col(KpiConstants.memberidColName).isin(eligiblePopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))



    val exclColAddedDf = if (optionalExclList.isEmpty) epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, lit(KpiConstants.zeroVal))
    else
      epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, when(epopColAddedDf.col(KpiConstants.memberidColName).isin(optionalExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val rexclColAddedDf = if (mandatoryExclList.isEmpty) exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, lit(KpiConstants.zeroVal))
    else
      exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, when(exclColAddedDf.col(KpiConstants.memberidColName).isin(mandatoryExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))

    val numColAddedDf = rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, lit(KpiConstants.zeroVal))
      .withColumn(KpiConstants.ncqaOutNumCol, when(($"${KpiConstants.memberidColName}".isin(numeratorPopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w150MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator2PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w151MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator3PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w152MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator4PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w153MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator5PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w154MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator6PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w155MeasureId), lit(KpiConstants.oneVal))
                                              .when(($"${KpiConstants.memberidColName}".isin(numerator7PopList: _*)) && $"${KpiConstants.ncqaOutMeasureCol}".===(KpiConstants.w156MeasureId), lit(KpiConstants.oneVal))
                                              .otherwise($"${KpiConstants.ncqaOutNumCol}"))


    val indAddedDf = numColAddedDf.withColumn(KpiConstants.ncqaOutIndCol, lit(KpiConstants.zeroVal))
                                  .withColumnRenamed(KpiConstants.memberidColName, KpiConstants.ncqaOutmemberIdCol)

    val resultDf = indAddedDf.select(KpiConstants.outncqaFormattedList.head, KpiConstants.outncqaFormattedList.tail: _*)
    resultDf

  }

  def ncqaWCCOutputDfCreation(spark: SparkSession, dfMap:mutable.Map[String, DataFrame]): DataFrame = {


    import spark.implicits._

    val totalPopDf = dfMap.get(KpiConstants.totalPopDfName).getOrElse(spark.emptyDataFrame)
    val eligiblePopDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val mandatoryExclDf = dfMap.get(KpiConstants.mandatoryExclDfname).getOrElse(spark.emptyDataFrame)
    val optionalExclDf = dfMap.get(KpiConstants.optionalExclDfName).getOrElse(spark.emptyDataFrame)
    val numerator1PopDf = dfMap.get(KpiConstants.numeratorDfName).getOrElse(spark.emptyDataFrame)
    val numerator2PopDf = dfMap.get(KpiConstants.numerator2DfName).getOrElse(spark.emptyDataFrame)
    val numerator3PopDf = dfMap.get(KpiConstants.numerator3DfName).getOrElse(spark.emptyDataFrame)



    val eligiblePopList = if(eligiblePopDf.count() > 0) eligiblePopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val mandatoryExclList = if(mandatoryExclDf.count() > 0) mandatoryExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val optionalExclList = if(optionalExclDf.count() > 0) optionalExclDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator1PopList = if(numerator1PopDf.count() > 0) numerator1PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator2PopList = if(numerator2PopDf.count() > 0) numerator2PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))
    val numerator3PopList = if(numerator3PopDf.count() > 0) numerator3PopDf.as[String].collectAsList() else mutableSeqAsJavaList(Seq(""))



    val epopColAddedDf = if (eligiblePopList.isEmpty) totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, lit(KpiConstants.zeroVal))
    else
      totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, when(totalPopDf.col(KpiConstants.memberidColName).isin(eligiblePopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))



    val exclColAddedDf = if (optionalExclList.isEmpty) epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, lit(KpiConstants.zeroVal))
    else
      epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, when(epopColAddedDf.col(KpiConstants.memberidColName).isin(optionalExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val rexclColAddedDf = if (mandatoryExclList.isEmpty) exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, lit(KpiConstants.zeroVal))
    else
      exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, when(exclColAddedDf.col(KpiConstants.memberidColName).isin(mandatoryExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))

    val numColAddedDf = rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, lit(KpiConstants.zeroVal))
      .withColumn(KpiConstants.ncqaOutNumCol,
        when(($"${KpiConstants.memberidColName}".isin(numerator1PopList: _*)) && ($"meas"===lit("WCC1A") || $"meas"===lit("WCC2A") ),lit(KpiConstants.oneVal))
          .when(($"${KpiConstants.memberidColName}".isin(numerator2PopList: _*)) && ($"meas"===lit("WCC1B") || $"meas"===lit("WCC2B") ),lit(KpiConstants.oneVal))
          .when(($"${KpiConstants.memberidColName}".isin(numerator3PopList: _*)) && ($"meas"===lit("WCC1C") || $"meas"===lit("WCC2C") ),lit(KpiConstants.oneVal))
          .otherwise(lit(KpiConstants.zeroVal)))


    val indAddedDf = numColAddedDf.withColumn(KpiConstants.ncqaOutIndCol, lit(KpiConstants.zeroVal))


    val resultDf = indAddedDf.select(KpiConstants.outncqaFormattedList.head, KpiConstants.outncqaFormattedList.tail: _*)
    resultDf

  }

  def add_ncqa_months(spark:SparkSession,startDate:Column, n:Int):Column ={

    import spark.implicits._
    val addDate = add_months(startDate,n)

    val result_date = when(dayofmonth(addDate).>(dayofmonth(startDate)), concat(lit(year(addDate)),lit("-"),lit(month(addDate)),lit("-"),lit(dayofmonth(startDate))).cast(DateType))
      .when((dayofmonth(addDate).===(lit(28))) && (month(addDate).===(lit(2))) && (dayofmonth(startDate).===(lit(29))) && (month(startDate).===(lit(2))),date_add(addDate,1))
      .otherwise(addDate)

    result_date
  }

  def contEnrollAndAllowableGapFilterFunction(spark:SparkSession, inputDf:DataFrame, argMap:Map[String, String]):DataFrame ={

    import spark.implicits._


    val startDate = argMap.get("start_date").getOrElse("")
    val endDate = argMap.get("end_date").getOrElse("")
    val anchorDate = argMap.get("anchor_date").getOrElse("")
    val gapCount = argMap.get("gapcount").getOrElse("").toInt
    val reqCovDays = argMap.get("reqCovDays").getOrElse("").toInt
    val chckVal = argMap.get("checkval").getOrElse("")


    val inputForContEnrolldf = inputDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
                                              KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
                                              KpiConstants.lobColName, KpiConstants.lobProductColName,
                                              KpiConstants.payerColName,KpiConstants.primaryPlanFlagColName,
                                              KpiConstants.dateofbirthColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName, lit(startDate).cast(DateType))
      .withColumn(KpiConstants.contenrollUppCoName, lit(endDate).cast(DateType))
      .withColumn(KpiConstants.anchorDateColName, lit(anchorDate).cast(DateType))


    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/

    var contEnrollStep1Df = spark.emptyDataFrame
    if(chckVal.equalsIgnoreCase("true")){

      contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
        || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
        ||($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}") && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}"))))
        .withColumn(KpiConstants.anchorflagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
          && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}")), lit(1)).otherwise(lit(0)))
        .withColumn(KpiConstants.contEdFlagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))
          && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}")), lit(1)).otherwise(lit(0)))

    }
    else{

      contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
        || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
        ||($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}") && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}"))))

    }



    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    var listDf = spark.emptyDataFrame

    if(chckVal.equalsIgnoreCase("true")){

      listDf = contEnrollStep1Df.groupBy($"${KpiConstants.memberidColName}")
        .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
          min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
          first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
          first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
          sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName))
        .filter((date_add($"max_mem_end_date",KpiConstants.days45).>=($"${KpiConstants.contenrollUppCoName}"))
          && (date_sub($"min_mem_start_date",KpiConstants.days45).<=($"${KpiConstants.contenrollLowCoName}"))
          &&($"${KpiConstants.anchorflagColName}").>(0))
        .select($"${KpiConstants.memberidColName}")

    }
    else{

      listDf = contEnrollStep1Df.groupBy($"${KpiConstants.memberidColName}")
        .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
          min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
          first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
          first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
        .filter((date_add($"max_mem_end_date",KpiConstants.days45).>=($"${KpiConstants.contenrollUppCoName}"))
          && (date_sub($"min_mem_start_date",KpiConstants.days45).<=($"${KpiConstants.contenrollLowCoName}")))
        .select($"${KpiConstants.memberidColName}")

    }


    val contEnrollStep2Df = contEnrollStep1Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select("df1.*")


    // contEnrollStep3Df.printSchema()
    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberidColName}").orderBy(org.apache.spark.sql.functions.col(s"${KpiConstants.memEndDateColName}").desc,org.apache.spark.sql.functions.col(s"${KpiConstants.memStartDateColName}"))


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep3Df = contEnrollStep2Df.withColumn(KpiConstants.overlapFlagColName, when(($"${KpiConstants.memStartDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memStartDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))
      && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))))
      ,lit(1))
      .when(($"${KpiConstants.memStartDateColName}".<(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)))
        && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1 ).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal)))
        ,lit(2)).otherwise(lit(0)))

      .withColumn(KpiConstants.coverageDaysColName,when($"${KpiConstants.overlapFlagColName}".===(0) ,datediff(when($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"), $"${KpiConstants.memEndDateColName}").otherwise($"${KpiConstants.contenrollUppCoName}")
        ,when($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}"), $"${KpiConstants.memStartDateColName}").otherwise($"${KpiConstants.contenrollLowCoName}"))+ 1 )
        .when($"${KpiConstants.overlapFlagColName}".===(2), datediff( when($"${KpiConstants.contenrollLowCoName}".>=(lag( $"${KpiConstants.memStartDateColName}",1).over(contWindowVal)), $"${KpiConstants.contenrollLowCoName}").otherwise(lag( $"${KpiConstants.memStartDateColName}",1).over(contWindowVal))
          ,$"${KpiConstants.memStartDateColName}")+1 )
        .otherwise(0))

      .withColumn(KpiConstants.countColName, when(when($"${KpiConstants.overlapFlagColName}".===(0), datediff(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
        .otherwise(0).>(1),lit(1))
        .otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep3Df.groupBy(KpiConstants.memberidColName)
      .agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
        sum($"${KpiConstants.coverageDaysColName}").alias(KpiConstants.coverageDaysColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
      .withColumn(KpiConstants.reqCovDaysColName, when(lit(reqCovDays).===(0),(datediff($"${KpiConstants.contenrollUppCoName}", $"${KpiConstants.contenrollLowCoName}")-44))
                                                  .otherwise(lit(reqCovDays)))



    val contEnrollmemDf = contEnrollStep5Df.filter(((($"${KpiConstants.countColName}") + (when(date_sub($"min_mem_start_date", 1).>=($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
      + (when(date_add($"max_mem_end_date", 1).<=($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
      && ($"${KpiConstants.coverageDaysColName}".>=($"${KpiConstants.reqCovDaysColName}")))
      .select(KpiConstants.memberidColName).distinct()



    var contEnrollDf = spark.emptyDataFrame
    if(chckVal.equalsIgnoreCase("true")){

      contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
        .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
        .select("df1.*")
    }
    else{

      contEnrollDf = inputForContEnrolldf.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
        .select("df1.*")

    }


    contEnrollDf
  }

}
