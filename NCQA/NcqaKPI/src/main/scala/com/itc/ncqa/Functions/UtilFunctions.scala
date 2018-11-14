package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, count, current_date, current_timestamp, date_add, date_format, datediff, expr, hash, lit, month, to_date, when, year}
import org.apache.spark.sql.types.DateType

import scala.util.Try
import scala.collection.JavaConversions._
import collection.mutable._

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
    //df1.show()
    val returnDf = df.except(df1)
    returnDf
  }


  /*Function Name:ageFilter
  * Input Argument: df-Datframe(Input DataFrame)
  * Input Argument: colName-String(date column name )
  * Input Argument: year-String(current year)
  * Input Argument: lower-String(lower age limit)
  * Input Argument: upper-String(upper age limit)
  * Output type: Dataframe
  * Description: returns a Dataframe that contains the elements which satisfies the age limit*/
  /*def ageFilter(df: DataFrame, colName:String, year:String,lower:String,upper:String):DataFrame={
    var current_date = year+"-12-31"

    val newDf1 = df.withColumn("curr_date",lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date",newDf1.col("curr_date").cast(DateType))
    //newDf2.withColumn("dateDiff", datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25 ).select("member_sk","dateDiff").distinct().show(200)
    val newdf3 = newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<=(upper.toInt))
    newdf3.drop("curr_date")
  }*/


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

      case "truetrue" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<=(upper.toInt))
      case "truefalse" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<(upper.toInt))
      case "falsetrue" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<=(upper.toInt))
      case "falsefalse" => newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"), newDf2.col(colName)) / 365.25).<(upper.toInt))
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
    * @param df (input dataframe where the filter has to do)
    * @param colName (date column name)
    * @param date1 (lower date (date should be in "dd-MMM-yyyy"))
    * @param date2 (upper date (date should be in "dd-MMM-yyyy"))
    * @return dataframe that contains the date column between the lower and upper dates
    * @usecase fUnction filter out the data whre the date column fall between the lower and upper dates
    */
  def dateBetweenFilter(df: DataFrame, colName: String, date1: String, date2: String): DataFrame = {

    val expressionString = colName + "BETWEEN " + date1 + " AND " + date2
    val newDf = df.filter(expressionString)
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
  def joinForCommonFilterFunction(spark: SparkSession, dimMemberDf: DataFrame, factClaimDf: DataFrame, factMembershipDf: DataFrame, dimLocationDf: DataFrame, refLobDf: DataFrame, facilityDf: DataFrame, lobName: String, measureTitle: String): DataFrame = {

    import spark.implicits._

    /*join dim_member,fact_claims,fact_membership and refLob.*/
    val joinedDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).join(factClaimDf.as("df3"),
      factMembershipDf.col(KpiConstants.memberskColName) === factClaimDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).join(refLobDf.as("df4"), factMembershipDf.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName), KpiConstants.innerJoinType).filter(refLobDf.col(KpiConstants.lobColName).===(lobName))
      .select("df1.member_sk", KpiConstants.arrayOfColumn: _*)


    /*join the joinedDf with dimLocation for getting facility_sk for doing the further join with dimFacility*/
    val joinedDimLocationDf = joinedDf.as("df1").join(dimLocationDf.as("df2"), factClaimDf.col(KpiConstants.locationSkColName) === dimLocationDf.col(KpiConstants.locationSkColName), KpiConstants.innerJoinType).select("df1.*", "df2.facility_sk")

    /*Join with dimFacility for getting facility_sk*/
    val facilityJoinedDf = joinedDimLocationDf.as("df1").join(facilityDf.as("df2"), joinedDimLocationDf.col(KpiConstants.facilitySkColName) === facilityDf.col(KpiConstants.facilitySkColName), KpiConstants.innerJoinType).select("df1.member_sk", "df1.date_of_birth_sk", "df1.gender", "df1.lob", "df1.product_plan_sk", "df2.facility_sk")

    /*Load the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*join the data with dim_date for getting calender date of dateofbirthsk*/
    val dobDateValAddedDf = facilityJoinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName), KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp").drop(KpiConstants.dobskColame)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = dobDateValAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")

    /*Finding the quality measure value from quality Measure Table*/
    val qualityMeasureSkList = DataLoadFunctions.qualityMeasureLoadFunction(spark, measureTitle).select("quality_measure_sk").as[String].collectAsList()

    val qualityMeasureSk = if (qualityMeasureSkList.isEmpty) "qualitymeasuresk" else qualityMeasureSkList(0)

    /*Adding an additional column(quality_measure_sk) to the calculated df */
    val finalResultantDf = resultantDf.withColumn(KpiConstants.qualityMsrSkColName, lit(qualityMeasureSk))
    finalResultantDf
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
  def dimMemberFactClaimHedisJoinFunction(spark: SparkSession, dimMemberDf: DataFrame, factClaimDf: DataFrame, refhedisDf: DataFrame, col1: String, joinType: String, measureId: String, valueSet: List[String], codeSystem: List[String]): DataFrame = {

    import spark.implicits._

    var newDf = spark.emptyDataFrame

    /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
    if (col1.equalsIgnoreCase("procedure_code")) {
      newDf = dimMemberDf.join(factClaimDf, dimMemberDf.col("member_sk") === factClaimDf.col("member_sk"), joinType).join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1") === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2") === refhedisDf.col("code") ||
        factClaimDf.col("PROCEDURE_HCPCS_CODE") === refhedisDf.col("code") || factClaimDf.col("CPT_II") === refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").isin(codeSystem: _*))).select(dimMemberDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
    }
    else {
      val code = codeSystem(0)
      newDf = dimMemberDf.join(factClaimDf, dimMemberDf.col("member_sk") === factClaimDf.col("member_sk"), joinType).join(refhedisDf, factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3") === refhedisDf.col("code") ||
        factClaimDf.col("DIAGNOSIS_CODE_4") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7") === refhedisDf.col("code") ||
        factClaimDf.col("DIAGNOSIS_CODE_8") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9") === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10") === refhedisDf.col("code"), joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet: _*)).&&(refhedisDf.col("codesystem").like(code))).select(dimMemberDf.col("member_sk"), factClaimDf.col("start_date_sk"), factClaimDf.col("provider_sk"), factClaimDf.col("admit_date_sk"), factClaimDf.col("discharge_date_sk"))
    }

    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val startDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    /* val admitDateValAddedDf = startDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.admit_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "admit_temp").drop("admit_date_sk")
     val dischargeDateValAddedDf = admitDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.discharge_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "discharge_temp").drop("discharge_date_sk")*/


    /*convert the start_date date into dd-MMM-yyy format*/
    val dateTypeDf = startDateValAddedDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")) //.withColumn("admit_date", to_date($"admit_temp", "dd-MMM-yyyy")).withColumn("discharge_date", to_date($"discharge_temp", "dd-MMM-yyyy")).drop( "start_temp","admit_temp","discharge_temp")
    dateTypeDf

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
  def hospiceMemberDfFunction(spark: SparkSession, dimMemberDf: DataFrame, factClaimDf: DataFrame, refhedisDf: DataFrame): DataFrame = {

    import spark.implicits._

    val newDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(refhedisDf.as("df3"), $"df2.PROCEDURE_HCPCS_CODE" === "df3.code", "cross").filter($"df3.code".===("G0155")).select("df1.member_sk", "start_date_sk")
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
  def commonOutputDfCreation(spark: SparkSession, dinominatorDf: DataFrame, dinoExclDf: DataFrame, numeratorDf: DataFrame, numExclDf: DataFrame, outValueList: List[List[String]], sourceName: String): DataFrame = {


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
      .withColumn(KpiConstants.outSourceNameColName, lit(sourceName))
      .withColumn(KpiConstants.outUserColName, lit(KpiConstants.userNameVal))
      .withColumn(KpiConstants.outRecCreateDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outRecUpdateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outDateSkColName, lit(dateSkVal))
      .withColumn(KpiConstants.outIngestionDateColName, lit(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")))
      .withColumn(KpiConstants.outMeasureIdColName,lit("ABA"))

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


}
