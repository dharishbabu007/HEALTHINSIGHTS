package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, month, to_date, when, year}
import org.apache.spark.sql.types.DateType

import scala.util.Try
import scala.collection.JavaConversions._

object UtilFunctions {



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


  def ageFilter(df: DataFrame, colName:String, year:String,lower:String,upper:String,lowerIncl:Boolean,upperIncl:Boolean):DataFrame={
    var current_date = year+"-12-31"

    val inclData = lowerIncl.toString + upperIncl.toString
    //println("inclData value:"+ inclData)
    val newDf1 = df.withColumn("curr_date",lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date",newDf1.col("curr_date").cast(DateType))
    //newDf2.withColumn("dateDiff", datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25 ).select("member_sk","dateDiff").distinct().show(200)
    val newdf3 = inclData match {

      case "truetrue" => newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<=(upper.toInt))
      case "truefalse" => newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<(upper.toInt))
      case "falsetrue" => newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<=(upper.toInt))
      case "falsefalse" => newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<(upper.toInt))
    }
    newdf3.drop("curr_date")
  }



  def mesurementYearFilter(df:DataFrame,colName:String,year:String,lower:Int,upper:Int):DataFrame={


    var current_date = year+"-12-31"
    val df1 = df.withColumn("curr",lit(current_date))
    val newDf = df1.withColumn("dateDiff",datediff(df1.col("curr"),df1.col(colName)))
    val newDf1 = newDf.filter(newDf.col("dateDiff").<=(upper).&&(newDf.col("dateDiff").>=(lower))).drop("curr","dateDiff")
    newDf1
  }


  def dateBetweenFilter(df:DataFrame,colName:String,date1:String,date2:String):DataFrame={

    val expressionString = colName +"BETWEEN " +date1 + " AND "+date2
    val newDf = df.filter(expressionString)
    newDf
  }


  def mostRececntHba1cTest(df:DataFrame,colName:String,year:String):DataFrame={

    var current_date = year+"-12-31"
    val df1 = df.withColumn("curr",lit(current_date))
    val newDf = df1.withColumn("dateDiff",datediff(df1.col("curr"),df1.col(colName)))
    val newDf1 = newDf.groupBy("member_sk").min("dateDiff")
    newDf1
  }




  /*Function for create data farme with needed columns for the initial filtering*/
  def joinForCommonFilterFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,factMembershipDf:DataFrame,dimLocationDf:DataFrame,refLobDf:DataFrame,facilityDf:DataFrame,lobName:String,measureTitle:String):DataFrame ={
    import spark.implicits._

   /*join dim_member,fact_claims,fact_membership and refLob.*/
    val joinedDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"),dimMemberDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(factClaimDf.as("df3"),
                   factMembershipDf.col(KpiConstants.memberskColName) === factClaimDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(refLobDf.as("df4"),factMembershipDf.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName),KpiConstants.innerJoinType).filter(refLobDf.col(KpiConstants.lobColName).===(lobName))
                   .select("df1.member_sk",KpiConstants.arrayOfColumn:_*)


    /*join the joinedDf with dimLocation for getting facility_sk for doing the further join with dimFacility*/
    val joinedDimLocationDf = joinedDf.as("df1").join(dimLocationDf.as("df2"),factClaimDf.col(KpiConstants.locationSkColName) === dimLocationDf.col(KpiConstants.locationSkColName),KpiConstants.innerJoinType).select("df1.*","df2.facility_sk")

    /*Join with dimFacility for getting facility_sk*/
    val facilityJoinedDf = joinedDimLocationDf.as("df1").join(facilityDf.as("df2"),joinedDimLocationDf.col(KpiConstants.facilitySkColName) === facilityDf.col(KpiConstants.facilitySkColName),KpiConstants.innerJoinType).select("df1.member_sk","df1.date_of_birth_sk","df1.gender","df1.lob","df1.product_plan_sk","df2.facility_sk")

    /*Load the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*join the data with dim_date for getting calender date of dateofbirthsk*/
    val dobDateValAddedDf = facilityJoinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp").drop(KpiConstants.dobskColame)

    /*convert the dob column to date format (dd-MMM-yyyy)*/
    val resultantDf = dobDateValAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")

    import spark.implicits._
    /*Finding the quality measure value from quality Measure Table*/
    val qualityMeasureSkList = DataLoadFunctions.qualityMeasureLoadFunction(spark,measureTitle).select("quality_measure_sk").as[String].collectAsList()

    val qualityMeasureSk = if(qualityMeasureSkList.isEmpty) "qualitymeasuresk" else qualityMeasureSkList(0)
    //print("qualityMeasureSk:"+ qualityMeasureSk)
    /*Adding an additional column(quality_measure_sk) to the calculated df */
    val finalResultantDf = resultantDf.withColumn(KpiConstants.qualityMsrSkColName,lit(qualityMeasureSk))
    finalResultantDf
  }






  def dimMemberFactClaimHedisJoinFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,refhedisDf:DataFrame,col1:String,joinType:String,measureId:String,valueSet:List[String],codeSystem:List[String]):DataFrame={

    import spark.implicits._

    var newDf = spark.emptyDataFrame

    /*Joining dim_member,fact_claim and ref_hedis tables based on the condition( either with primary_diagnosis or with procedure_code)*/
    if(col1.equalsIgnoreCase("procedure_code"))
    {
      newDf = dimMemberDf.join(factClaimDf,dimMemberDf.col("member_sk") === factClaimDf.col("member_sk"),joinType).join(refhedisDf,factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER1")=== refhedisDf.col("code") || factClaimDf.col("PROCEDURE_CODE_MODIFIER2")=== refhedisDf.col("code") ||
        factClaimDf.col("PROCEDURE_HCPCS_CODE")=== refhedisDf.col("code") || factClaimDf.col("CPT_II")=== refhedisDf.col("code") || factClaimDf.col("CPT_II_MODIFIER")=== refhedisDf.col("code") ,joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet:_*)).&&(refhedisDf.col("codesystem").isin(codeSystem:_*))).select(dimMemberDf.col("member_sk"),factClaimDf.col("start_date_sk"),factClaimDf.col("provider_sk"),factClaimDf.col("admit_date_sk"),factClaimDf.col("discharge_date_sk"))
    }
    else
    {
      val code = codeSystem(0)
      newDf = dimMemberDf.join(factClaimDf,dimMemberDf.col("member_sk") === factClaimDf.col("member_sk"),joinType).join(refhedisDf,factClaimDf.col(col1) === refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_2")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_3")=== refhedisDf.col("code")||
      factClaimDf.col("DIAGNOSIS_CODE_4")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_5")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_6")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_7")=== refhedisDf.col("code") ||
      factClaimDf.col("DIAGNOSIS_CODE_8")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_9")=== refhedisDf.col("code") || factClaimDf.col("DIAGNOSIS_CODE_10")=== refhedisDf.col("code"),joinType).filter(refhedisDf.col("measureid").===(measureId).&&(refhedisDf.col("valueset").isin(valueSet:_*)).&&(refhedisDf.col("codesystem").like(code))).select(dimMemberDf.col("member_sk"),factClaimDf.col("start_date_sk"),factClaimDf.col("provider_sk"),factClaimDf.col("admit_date_sk"),factClaimDf.col("discharge_date_sk"))
    }

    /*Loading the dim_date table*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*getting calender dates fro start_date_sk,admit_date_sk and discharge_date_sk*/
    val startDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val admitDateValAddedDf = startDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.admit_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "admit_temp").drop("admit_date_sk")
    val dischargeDateValAddedDf = admitDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.discharge_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "discharge_temp").drop("discharge_date_sk")

    /*convert all the dates into dd-MMM-yyy format*/
    val dateTypeDf = dischargeDateValAddedDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).withColumn("admit_date", to_date($"admit_temp", "dd-MMM-yyyy")).withColumn("discharge_date", to_date($"discharge_temp", "dd-MMM-yyyy")).drop( "start_temp","admit_temp","discharge_temp")
    dateTypeDf
  }







def removeHeaderFromDf(df:DataFrame,headervalues:Array[String],colName:String):DataFrame={

  val df1 = df.filter(df.col(colName).isin(headervalues:_*))
  //df1.show()
  val returnDf = df.except(df1)
  returnDf
}




  def outputDfCreation(spark:SparkSession,superDf:DataFrame,dfexclusionDf:DataFrame,numDf:DataFrame,dimMemberDf:DataFrame,measVal:String):DataFrame={


    //var resultantDf = spark.emptyDataFrame

    import spark.implicits._
    val exclList = dfexclusionDf.as("df1").join(dimMemberDf.as("df2"),dfexclusionDf.col("member_sk") === dimMemberDf.col("member_sk")).select("member_id").as[String].collectAsList()
    val numList =numDf.as("df1").join(dimMemberDf.as("df2"),numDf.col("member_sk") === dimMemberDf.col("member_sk")).select("df2.member_id").as[String].collectAsList()
    val outputFormat = superDf.withColumn("Meas",lit(measVal)).withColumn("Epop",lit(1)).withColumnRenamed("lob_name","Payer").withColumnRenamed("member_id","MemID")
    val exclusionAdded = outputFormat.withColumn("Excl", when(outputFormat.col("MemID").isin(exclList:_*),lit(1)).otherwise(lit(0)))
    val numAdded = exclusionAdded.withColumn("Num",when(exclusionAdded.col("MemID").isin(numList:_*),lit(1)).otherwise(lit(0))).withColumn("RExcl",lit(0)).withColumn("Ind",lit(0))
    val formattedOutPutDf = numAdded.select("MemID","Meas","Payer","Epop","Excl","Num","RExcl","Ind")
    formattedOutPutDf

  }












  def commonOutputDfCreation(spark:SparkSession,dinominatorDf:DataFrame,dinoExclDf:DataFrame,numeratorDf:DataFrame,numExclDf:DataFrame,outValueList:List[List[String]]/*dimMemberDf:DataFrame,measVal:String*/):DataFrame={


    //var resultantDf = spark.emptyDataFrame

    import spark.implicits._
    val dinoExclList = dinoExclDf.as[String].collectAsList()
    val numList = numeratorDf.as[String].collectAsList()
    val numExclList = if (numExclDf.count()> 0 ) List(numExclDf.as[String].collectAsList()) else List("")
    val numeratorValueSet = outValueList(0)
    val dinoExclValueSet = outValueList(1)
    val numExclValueSet = outValueList(2)


    /*Adding InDinominator,InDinominatorExclusion,Numearator and Numerator Exclusion columns*/
    val inDinoAddedDf = dinominatorDf.withColumn(KpiConstants.outInDinoColName,lit(KpiConstants.yesVal))
    val inDinoExclAddedDf = inDinoAddedDf.withColumn(KpiConstants.outInDinoExclColName,when(inDinoAddedDf.col(KpiConstants.memberskColName).isin(dinoExclList:_*),lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))
    val inNumeAddedDf = inDinoExclAddedDf.withColumn(KpiConstants.outInNumColName,when(inDinoExclAddedDf.col(KpiConstants.memberskColName).isin(numList:_*),lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))
    val inNumExclAddedDf = if (numExclDf.count()> 0) inNumeAddedDf.withColumn(KpiConstants.outInNumExclColName,when(inNumeAddedDf.col(KpiConstants.memberskColName).isin(numExclList:_*),lit(KpiConstants.yesVal)).otherwise(lit(KpiConstants.noVal)))
                                                      else
                                                      inNumeAddedDf.withColumn(KpiConstants.outInNumExclColName,lit(KpiConstants.noVal))


    /*Adding dinominator exception and numerator exception columns*/
    val dinoNumExceptionAddedDf = inNumExclAddedDf.withColumn(KpiConstants.outInDinoExcColName,lit(KpiConstants.noVal)).withColumn(KpiConstants.outInNumExcColName,lit(KpiConstants.noVal))


    /*Adding numerator Reasons columns*/
    val numeratorValueSetColAddedDf = dinoNumExceptionAddedDf.withColumn(KpiConstants.outNu1ReasonColName,when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal),KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length -1 < 0) KpiConstants.emptyStrVal else numeratorValueSet.get(0)))
                                                             .withColumn(KpiConstants.outNu2ReasonColName,when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal),KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length -1 < 1) KpiConstants.emptyStrVal else numeratorValueSet.get(1)))
                                                             .withColumn(KpiConstants.outNu3ReasonColName,when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal),KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length -1 < 2) KpiConstants.emptyStrVal else numeratorValueSet.get(2)))
                                                             .withColumn(KpiConstants.outNu4ReasonColName,when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal),KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length -1 < 3) KpiConstants.emptyStrVal else numeratorValueSet.get(3)))
                                                             .withColumn(KpiConstants.outNu5ReasonColName,when(dinoNumExceptionAddedDf.col(KpiConstants.outInNumColName).===(KpiConstants.yesVal),KpiConstants.emptyStrVal).otherwise(if (numeratorValueSet.length -1 < 4) KpiConstants.emptyStrVal else numeratorValueSet.get(4)))


    /*Adding Dinominator Exclusion Reason columns */
    val dinoExclValueSetColAddedDf = numeratorValueSetColAddedDf.withColumn(KpiConstants.outDinoExcl1ReasonColName,when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length -1 < 0) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))
                                                                .withColumn(KpiConstants.outDinoExcl2ReasonColName,when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length -1 < 1) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))
                                                                .withColumn(KpiConstants.outDinoExcl3ReasonColName,when(numeratorValueSetColAddedDf.col(KpiConstants.outInDinoExclColName).===(KpiConstants.noVal), KpiConstants.emptyStrVal).otherwise(if (dinoExclValueSet.length -1 < 2) KpiConstants.emptyStrVal else dinoExclValueSet.get(0)))


    /*Adding Numerator Exclusion reason Columns*/
    val numoExclValueSetColAddedDf = dinoExclValueSetColAddedDf.withColumn(KpiConstants.outNumExcl1ReasonColName, when(dinoExclValueSetColAddedDf.col(KpiConstants.outInNumExcColName).===(KpiConstants.noVal),KpiConstants.emptyStrVal).otherwise(if (numExclValueSet.length -1 < 0) KpiConstants.emptyStrVal else numExclValueSet.get(0)))
                                                               .withColumn(KpiConstants.outNumExcl1ReasonColName, when(dinoExclValueSetColAddedDf.col(KpiConstants.outInNumExcColName).===(KpiConstants.noVal),KpiConstants.emptyStrVal).otherwise(if (numExclValueSet.length -1 < 0) KpiConstants.emptyStrVal else numExclValueSet.get(0)))

    numoExclValueSetColAddedDf
  }









def hospiceMemberDfFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,refhedisDf:DataFrame):DataFrame={

  import spark.implicits._

  val newDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").join(refhedisDf.as("df3"),$"df2.PROCEDURE_HCPCS_CODE" === "df3.code","cross").filter($"df3.code".===("G0155")).select("df1.member_sk","start_date_sk")
  val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
  val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
  val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop( "start_temp")
  dateTypeDfForDinoExcl.select("member_sk","start_date")
}



}
