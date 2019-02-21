package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.util.Try
import scala.collection.JavaConversions._
import collection.mutable._
import scala.collection.mutable


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
    println("df and returndf count:"+ df.count()+","+ returnDf.count()+","+ df1.count())
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

/*
  def initialJoinFunction(spark:SparkSession, dfMap:mutable.Map[String, DataFrame]):DataFrame ={


    import spark.implicits._

    val dimMemberDf = dfMap.get(KpiConstants.dimMemberTblName).getOrElse(spark.emptyDataFrame)
    val factMembershipDf = dfMap.get(KpiConstants.factMembershipTblName).getOrElse(spark.emptyDataFrame)
    val dimProductPlanDf = dfMap.get(KpiConstants.dimProductTblName).getOrElse(spark.emptyDataFrame)
    val refLobDf = dfMap.get(KpiConstants.refLobTblName).getOrElse(spark.emptyDataFrame)
    val factMemAttrDf = dfMap.get(KpiConstants.factMemAttrTblName).getOrElse(spark.emptyDataFrame)
    val factMonMemDf = dfMap.get(KpiConstants.factMonMembershipTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf = dfMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    /*join dim_member,fact_membership,dim_product_plan,refLob,fact_mem_attribution*/
    val joinedDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .join(dimProductPlanDf.as("df3"), $"df2.${KpiConstants.productplanSkColName}" === $"df3.${KpiConstants.productplanSkColName}", KpiConstants.innerJoinType)
                                               .join(refLobDf.as("df4"), $"df3.${KpiConstants.lobIdColName}" === $"df4.${KpiConstants.lobIdColName}", KpiConstants.innerJoinType)
                                               .join(factMemAttrDf.as("df5"), $"df1.${KpiConstants.memberskColName}" === $"df5.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .join(factMonMemDf.as("df6"), $"df1.${KpiConstants.memberskColName}" === $"df6.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .filter((factMembershipDf.col(KpiConstants.considerationsColName).===(KpiConstants.yesVal))
                                                    && (factMembershipDf.col(KpiConstants.memPlanStartDateSkColName).isNotNull)
                                                    && (factMembershipDf.col(KpiConstants.memPlanEndDateSkColName).isNotNull))
                                               .select(s"df1.${KpiConstants.memberskColName}",s"df1.${KpiConstants.dobskColame}", s"df1.${KpiConstants.genderColName}", s"df2.${KpiConstants.productplanSkColName}",
                                                             s"df2.${KpiConstants.memPlanStartDateSkColName}",s"df2.${KpiConstants.memPlanEndDateSkColName}", s"df2.${KpiConstants.benefitMedicalColname}",s"df3.${KpiConstants.lobProductColName}",
                                                             s"df4.${KpiConstants.lobColName}", s"df4.${KpiConstants.lobNameColName}", s"df5.${KpiConstants.providerSkColName}", s"df6.${KpiConstants.ltiFlagColName}",
                                                             s"df6.${KpiConstants.lisPremiumSubColName}", s"df6.${KpiConstants.orgreasentcodeColName}")






    /*join the data with dim_date for getting calender date of dateofbirthsk,member_plan_start_date_sk,member_plan_end_date_sk*/
    val dobDateValAddedDf = joinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                     .select($"df1.*", $"df2.calendar_date")
                                                     .withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp")
                                                     .drop(KpiConstants.dobskColame)

    //dobDateValAddedDf.show(50)
    val memStartDateAddedDf = dobDateValAddedDf.as("df1").join(dimDateDf.as("df2"), dobDateValAddedDf.col(KpiConstants.memPlanStartDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                                .select($"df1.*", $"df2.calendar_date")
                                                                .withColumnRenamed(KpiConstants.calenderDateColName, "mem_start_temp")
                                                                .drop(KpiConstants.memPlanStartDateSkColName)


    val memEndDateAddedDf = memStartDateAddedDf.as("df1").join(dimDateDf.as("df2"), memStartDateAddedDf.col(KpiConstants.memPlanEndDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType)
                                                                .select($"df1.*", $"df2.calendar_date")
                                                                .withColumnRenamed(KpiConstants.calenderDateColName, "mem_end_temp")
                                                                .drop(KpiConstants.memPlanEndDateSkColName)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = memEndDateAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy"))
                                       .withColumn(KpiConstants.memStartDateColName, to_date($"mem_start_temp", "dd-MMM-yyyy"))
                                       .withColumn(KpiConstants.memEndDateColName, to_date($"mem_end_temp", "dd-MMM-yyyy")).drop("dob_temp","mem_start_temp","mem_end_temp")


    resultantDf
  }
*/
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


  /**
    *
    * @param spark
    * @param inputDf
    * @param startDate
    * @param endDate
    * @param lob_name
    * @return
    */
  def contEnrollAndAllowableGapFilter(spark:SparkSession, inputDf:DataFrame, formatName:String, argMap:Map[String, String] /*,startDate:String, endDate:String,anchDate:String,benefitColName:String,lob_name:String*/):DataFrame ={

    import spark.implicits._


    /*deciding allowable length based on the lob name*/
    val lobName = argMap.get(KpiConstants.lobNameKeyName).getOrElse("")

    val allowableGapDays = lobName match {

      case KpiConstants.commercialLobName => KpiConstants.days45

      case KpiConstants.medicareLobName => KpiConstants.days45

      case KpiConstants.medicaidLobName => KpiConstants.days45

    }

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
                                                  inputDf.withColumn(KpiConstants.contenrollLowCoName, add_months($"${KpiConstants.dobColName}", (ageContStart.toInt * 12)))
                                                         .withColumn(KpiConstants.contenrollUppCoName, add_months($"${KpiConstants.dobColName}", (ageContEnd.toInt * 12)))
                                                         .withColumn(KpiConstants.anchorDateColName, add_months($"${KpiConstants.dobColName}", (ageAnchor.toInt * 12)))

                                        }


    }

      /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
      val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
                                                   || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))))

      //contEnrollStep1Df.show(50)
      /*Step2(select only the entries whoose benefit is the one that required) */
      val benefitColName = argMap.get(KpiConstants.benefitKeyName).getOrElse("")
     /* val noBenefitDf = contEnrollStep1Df.filter($"$benefitColName".===("N")).select(KpiConstants.memberskColName)
      val contEnrollStep2Df = contEnrollStep1Df.as("df1").join(noBenefitDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                                                .filter($"df2.${KpiConstants.memberskColName}".isNull)
                                                                .select("df1.*")*/


      val contEnrollStep2Df = contEnrollStep1Df.withColumn(benefitColName, when($"$benefitColName".===(KpiConstants.yesVal), 1).otherwise(0))
      //contEnrollStep2Df.show(50)

      /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
      val listDf = contEnrollStep2Df.groupBy($"${KpiConstants.memberskColName}")
                                    .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName), min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
                                    first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
                                   .filter(((date_add($"max_mem_end_date",allowableGapDays+1).>=($"${KpiConstants.contenrollUppCoName}")) && (date_sub($"min_mem_start_date",allowableGapDays+1).<=($"${KpiConstants.contenrollLowCoName}"))))
                                   .select($"${KpiConstants.memberskColName}")
      val contEnrollStep3Df = contEnrollStep2Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                                .select("df1.*")
      //contEnrollStep3Df.show(50)

      /*step4()*/
      /*window function creation based on partioned by member_sk and order by mem_start_date*/
      val contWindowVal = Window.partitionBy(s"${KpiConstants.memberskColName}").orderBy(s"${KpiConstants.memStartDateColName}")


      /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
       anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
       count(if date_diff>1 1, otherwise 0) over window*/
      val contEnrollStep4Df = contEnrollStep3Df.withColumn(KpiConstants.datediffColName, datediff(lead($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
                                               .withColumn(KpiConstants.anchorflagColName,when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
                                                                                                && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}"))
                                                                                                && $"${KpiConstants.lobColName}".===(lobName), lit(1)).otherwise(lit(0)))
                                               .withColumn(KpiConstants.countColName, when($"${KpiConstants.datediffColName}".>(1),lit(1)).otherwise(lit(0)) )

     // contEnrollStep4Df.show(50)

      val contEnrollStep5Df = contEnrollStep4Df.groupBy(KpiConstants.memberskColName).agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
                                                                                      max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
                                                                                      max($"${KpiConstants.datediffColName}").alias(KpiConstants.maxDateDiffColName),
                                                                                      sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
                                                                                      sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName),
                                                                                      first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
                                                                                      first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
                                                                                      sum($"$benefitColName").alias(KpiConstants.sumBenefitColName),
                                                                                      count($"$benefitColName").alias(KpiConstants.countBenefitColName))
    //contEnrollStep5Df.show(50)
      //contEnrollStep5Df.withColumn("value", lit((($"${KpiConstants.countColName}") + (when(date_sub($"min_mem_start_date", 1).>(startDate),lit(1)).otherwise(lit(0))) + (when(date_add($"max_mem_end_date", 1).<(endDate),lit(1)).otherwise(lit(0)))))).show(50)
      val contEnrollDf = contEnrollStep5Df.filter((($"${KpiConstants.maxDateDiffColName}" - 1).<=(allowableGapDays) || ($"${KpiConstants.maxDateDiffColName}" - 1).isNull)
                                                 && ($"${KpiConstants.anchorflagColName}".>(0))
                                                 && ((($"${KpiConstants.countColName}")
                                                        + (when(date_sub($"min_mem_start_date", 1).>($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
                                                        + (when(date_add($"max_mem_end_date", 1).<($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
                                                 && ($"${KpiConstants.sumBenefitColName}".===($"${KpiConstants.countBenefitColName}")))
                                          .select(KpiConstants.memberskColName)

      //contEnrollDf.show(50)
      contEnrollDf
   /* }
   */

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


  def findFralityMembers(spark:SparkSession,argMap:mutable.Map[String,DataFrame], year:String, lowerAge:String,upperAge:String):DataFrame ={


    import spark.implicits._

    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem
    val contEnrollDf = argMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val factClaimDf  = argMap.get(KpiConstants.factClaimTblName).getOrElse(spark.emptyDataFrame)
    val refHedisDf   = argMap.get(KpiConstants.refHedisTblName).getOrElse(spark.emptyDataFrame)
    val dimDateDf    = argMap.get(KpiConstants.dimDateTblName).getOrElse(spark.emptyDataFrame)

    val ageMoreThanDf = UtilFunctions.ageFilter(contEnrollDf,KpiConstants.dobColName,year,lowerAge, upperAge,KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
                                     .select(KpiConstants.memberskColName)

    val argmapForFralityExclusion = mutable.Map(KpiConstants.eligibleDfName -> ageMoreThanDf, KpiConstants.factClaimTblName -> factClaimDf,
                                                KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf)
    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForFralityExclusion,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,fralityValList,primaryDiagnosisCodeSystem)
    val fralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement0Val)
                                       .select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForFralityExclusion,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,fralityValList,fralityCodeSystem)
    val fralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val)
                                       .select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = fralityAsDiagDf.union(fralityAsProcDf)
    fralityDf
  }



  /*New Functions that will be used in measures*/

  /**
    *
    * @param member
    * @param dates
    * @return
    */
  def getMembers(member:String,dates:Array[Long]):(String,String) ={

    var a,b,c, counter = 0

    for(i <- 0 to dates.length){
      if(i == 0) a= i else a = c

      b = i+1
      val diff = dates(b) - dates(a)

      if(diff >= 1209600000){
        counter += 1
        if(counter>=2) {
          return (member, "Y")
        }
        c = b
      }
      else {
        c = a
      }

    }
    return (member, "N")
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


    val df = membershipDf.as("df1").join(visitDf.as("df2"), KpiConstants.memberidColName)
    val resultantDf = df.withColumn(KpiConstants.dobColName, to_date($"${KpiConstants.dobColName}", "yyyy-mm-dd"))
                .withColumn(KpiConstants.memStartDateColName, to_date($"${KpiConstants.memStartDateColName}", "yyyy-mm-dd"))
                .withColumn(KpiConstants.memEndDateColName, to_date($"${KpiConstants.memEndDateColName}", "yyyy-mm-dd"))
                .withColumn(KpiConstants.serviceDateColName, to_date($"${KpiConstants.serviceDateColName}", "yyyy-mm-dd"))
                .withColumn(KpiConstants.admitDateColName, when($"${KpiConstants.admitDateColName}".isNotNull,to_date($"${KpiConstants.admitDateColName}", "yyyy-mm-dd")))
                .withColumn(KpiConstants.dischargeDateColName, when($"${KpiConstants.dischargeDateColName}".isNotNull,to_date($"${KpiConstants.dischargeDateColName}", "yyyy-mm-dd")))


    //df1.printSchema()

    resultantDf
  }


  /**
    *
    * @param spark
    * @param dfMap
    * @param measureId
    * @param valueset
    * @param codeSystem
    * @return
    */
  def joinWithRefHedisFunction(spark:SparkSession, dfMap:mutable.Map[String,DataFrame], valueset:List[String],codeSystem:List[String]):DataFrame ={

    import spark.implicits._

    val factClaimDf = dfMap.get(KpiConstants.eligibleDfName).getOrElse(spark.emptyDataFrame)
    val refhedisDf = dfMap.get(KpiConstants.refHedisTblName).getOrElse(spark.emptyDataFrame)
    val refmedDf = dfMap.get(KpiConstants.refmedValueSetTblName).getOrElse(spark.emptyDataFrame)

    var inDf = spark.emptyDataFrame

    for (item <- codeSystem){

      val df = item match {


        case KpiConstants.modifierCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.proccodemod1ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.proccodemod2ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.proccode2mod1ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.proccodemod2ColName}" === $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
                                                                        .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                        .select("df1.*")

        case KpiConstants.cptCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.proccodeColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                              || $"df1.${KpiConstants.obstestColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                    .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                    .select("df1.*")

        case KpiConstants.cptCatIIVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.proccode2ColName}" === $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
                                                                     .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                     .select("df1.*")

        case KpiConstants.cvxCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.medcodeColName}" === $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
                                                                    .filter(($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*)) && ($"df1.${KpiConstants.medcodeflagColName}".===("C")))
                                                                    .select("df1.*")

        case KpiConstants.hcpsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.prochcpscodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                     .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                     .select("df1.*")

        case KpiConstants.icd9cmCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.primaryDiagnosisColname}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode2ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode3ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode4ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode5ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode6ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode7ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode8ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode9ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                 || $"df1.${KpiConstants.diagcode10ColName}" === $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
                                                                       .filter(($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*)) && ($"df1.${KpiConstants.icdflagColName}".===("9")))
                                                                       .select("df1.*")


        case KpiConstants.icd10cmCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.primaryDiagnosisColname}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode2ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode3ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode4ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode5ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode6ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode7ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode8ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode9ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.diagcode10ColName}" === $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
                                                                        .filter(($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*)) && ($"df1.${KpiConstants.icdflagColName}".===("X")))
                                                                        .select("df1.*")

        case KpiConstants.icd9pcsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.priicdprocColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.icdproc2ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.icdproc3ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.icdproc4ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.icdproc5ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                  || $"df1.${KpiConstants.icdproc6ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                        .filter(($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*)) && ($"df1.${KpiConstants.icdflagColName}".===("9")))
                                                                        .select("df1.*")

        case KpiConstants.icd10pcsCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.priicdprocColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.icdproc2ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.icdproc3ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.icdproc4ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.icdproc5ColName}" === $"df2.${KpiConstants.codeColName}"
                                                                                                                   || $"df1.${KpiConstants.icdproc6ColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                         .filter(($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*)) && ($"df1.${KpiConstants.icdflagColName}".===("X")))
                                                                         .select("df1.*")


        case KpiConstants.loincCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.loincCodeVal}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                      .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                      .select("df1.*")


        case KpiConstants.posCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.poscodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                    .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                    .select("df1.*")

        case KpiConstants.rxnormCodeVal => factClaimDf.as("df1").join(refmedDf.as("df2"), $"df1.${KpiConstants.medcodeColName}" === $"df2.${KpiConstants.ndcCodeColName}"
                                                                                                               || $"df1.${KpiConstants.ndcCodeColName}" === $"df2.${KpiConstants.ndcCodeColName}", KpiConstants.innerJoinType)
                                                                       .filter($"df2.${KpiConstants.medicatiolListColName}".isin(valueset:_*))
                                                                       .select("df1.*")

        case KpiConstants.snomedctCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.revenuecodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                         .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                         .select("df1.*")

        case KpiConstants.ubrevCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.revenuecodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                      .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                      .select("df1.*")

        case KpiConstants.ubtobCodeVal => factClaimDf.as("df1").join(refhedisDf.as("df2"), $"df1.${KpiConstants.billtypecodeColName}" === $"df2.${KpiConstants.codeColName}", KpiConstants.innerJoinType)
                                                                      .filter($"df2.${KpiConstants.valuesetColName}".isin(valueset:_*))
                                                                      .select("df1.*")


      }

      if(inDf == spark.emptyDataFrame){

        inDf = df
      }
      else{

        inDf = inDf.union(df)
      }
    }

    inDf
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
                             totalPopDf.withColumn(KpiConstants.ncqaOutEpopCol, when(totalPopDf.col(KpiConstants.memberskColName).isin(eligiblePopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))



    val exclColAddedDf = if (optionalExclList.isEmpty) epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, lit(KpiConstants.zeroVal))
                         else
                             epopColAddedDf.withColumn(KpiConstants.ncqaOutExclCol, when(epopColAddedDf.col(KpiConstants.memberskColName).isin(optionalExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val rexclColAddedDf = if (mandatoryExclList.isEmpty) exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, lit(KpiConstants.zeroVal))
                          else
                              exclColAddedDf.withColumn(KpiConstants.ncqaOutRexclCol, when(exclColAddedDf.col(KpiConstants.memberskColName).isin(mandatoryExclList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val numColAddedDf = if (numeratorPopList.isEmpty) rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, lit(KpiConstants.zeroVal))
                        else
                            rexclColAddedDf.withColumn(KpiConstants.ncqaOutNumCol, when(exclColAddedDf.col(KpiConstants.memberskColName).isin(numeratorPopList: _*), lit(KpiConstants.oneVal)).otherwise(lit(KpiConstants.zeroVal)))


    val measAddedDf = numColAddedDf.withColumn(KpiConstants.ncqaOutMeasureCol,lit(measureId))

    val resultDf = measAddedDf.select(KpiConstants.outncqaFormattedList.head, KpiConstants.outncqaFormattedList.tail: _*)
    resultDf

  }


}
