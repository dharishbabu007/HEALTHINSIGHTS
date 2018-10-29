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
  def ageFilter(df: DataFrame, colName:String, year:String,lower:String,upper:String):DataFrame={
    var current_date = year+"-12-31"

    val newDf1 = df.withColumn("curr_date",lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date",newDf1.col("curr_date").cast(DateType))
    //newDf2.withColumn("dateDiff", datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25 ).select("member_sk","dateDiff").distinct().show(200)
    val newdf3 = newDf2.filter((datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).>=(lower.toInt) && (datediff(newDf2.col("curr_date"),newDf2.col(colName))/365.25).<=(upper.toInt))
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




  def joinForCommonFilterFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,factMembershipDf:DataFrame,dimLocationDf:DataFrame,refLobDf:DataFrame,lobName:String,measureTitle:String):DataFrame ={
    import spark.implicits._

    val joinedDf = dimMemberDf.as("df1").join(factMembershipDf.as("df2"),dimMemberDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(factClaimDf.as("df3"),
                   factMembershipDf.col(KpiConstants.memberskColName) === factClaimDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(refLobDf.as("df4"),factMembershipDf.col(KpiConstants.lobIdColName) === refLobDf.col(KpiConstants.lobIdColName),KpiConstants.innerJoinType).filter(refLobDf.col(KpiConstants.lobColName).===(lobName))
                   .select("df1.member_sk",KpiConstants.arrayOfColumn:_*)

    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val dobDateValAddedDf = joinedDf.as("df1").join(dimDateDf.as("df2"), joinedDf.col(KpiConstants.dobskColame) === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, "dob_temp").drop(KpiConstants.dobskColame)
    val resultantDf = dobDateValAddedDf.withColumn(KpiConstants.dobColName, to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark,measureTitle).toString()
    val finalResultantDf = resultantDf.withColumn(KpiConstants.qualityMsrSkColName,lit(qualityMeasureSk))
    finalResultantDf
  }






  def dimMemberFactClaimHedisJoinFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,refhedisDf:DataFrame,col1:String,joinType:String,measureId:String,valueSet:List[String],codeSystem:List[String]):DataFrame={

    import spark.implicits._

    var newDf = spark.emptyDataFrame
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

    val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
    val startDateValAddedDf = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val admitDateValAddedDf = startDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.admit_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "admit_temp").drop("admit_date_sk")
    val dischargeDateValAddedDf = admitDateValAddedDf.as("df1").join(dimDateDf.as("df2"), $"df1.discharge_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "discharge_temp").drop("discharge_date_sk")
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




def hospiceMemberDfFunction(spark:SparkSession,dimMemberDf:DataFrame,factClaimDf:DataFrame,refhedisDf:DataFrame):DataFrame={

  import spark.implicits._

  val newDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").join(refhedisDf.as("df3"),$"df2.PROCEDURE_HCPCS_CODE" === "df3.code","cross").filter($"df3.code".===("G0155")).select("df1.member_sk","start_date_sk")
  val dimDateDf = spark.sql("select date_sk,calendar_date from ncqa_sample.dim_date")
  val startDateValAddedDfForDinoExcl = newDf.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
  val dateTypeDfForDinoExcl = startDateValAddedDfForDinoExcl.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop( "start_temp")
  dateTypeDfForDinoExcl.select("member_sk","start_date")
}



}
