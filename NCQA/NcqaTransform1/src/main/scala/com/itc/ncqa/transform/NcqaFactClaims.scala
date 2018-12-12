package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

object NcqaFactClaims {

  def main(args: Array[String]): Unit = {


    val schemaFilePath = args(0)
    val sourceDbName = args(1)
    val targetDbName = args(2)
    val kpiName = args(3)
    TransformConstants.setSourceDbName(sourceDbName)
    TransformConstants.setTargetDbName(targetDbName)

    val config = new SparkConf().setAppName("Ncqa FactMembershipCreation").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    val schemaPath = args(0)

    import spark.implicits._

    val schemaRdd = spark.sparkContext.textFile(schemaPath, 1)



    /*val queryString = "select * from ncqa_intermediate.Visit"
    val visitDf = spark.sql(queryString)*/
    val visitDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.visitTableName,kpiName)
    //visitDf.printSchema()

    /*adding Date_E by giving the column value of Date_Disch*/
    val endDateColumnaddedFactClaimsDf = visitDf.withColumn("Date_E",$"Date_Disch")

    /*formatting all the date columns in dd-mm-yyyy fromat*/
    val dateFormattedFactClaimsDf = endDateColumnaddedFactClaimsDf.withColumn("Date_S",date_format($"Date_S","dd-MMM-yyyy")).withColumn("Date_Adm",date_format($"Date_Adm","dd-MMM-yyyy")).withColumn("Date_Disch",date_format($"Date_Disch","dd-MMM-yyyy")).withColumn("Date_E",date_format($"Date_E","dd-MMM-yyyy"))

    /*join Df with different tables to get the sks*/
    //val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
    val dimMemberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)
    val memberSkAddedFactClaimDf = dateFormattedFactClaimsDf.as("df1").join(dimMemberDf.as("df2"),$"df1.MemID" === $"df2.member_id").select($"df1.*",$"df2.member_sk").withColumnRenamed("member_sk","MEMBER_SK").drop("member_id")
    //val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val startDateSkFactClaimDf = memberSkAddedFactClaimDf.as("df1").join(dimDateDf.as("df2"),$"df1.Date_S" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","START_DATE_SK").drop("Date_S")
    val admitDateSkFactClaimDf = startDateSkFactClaimDf.as("df1").join(dimDateDf.as("df2"),$"df1.Date_Adm" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","ADMIT_DATE_SK").drop("Date_Adm")
    val dischargeDateSkFactClaimDf = admitDateSkFactClaimDf.as("df1").join(dimDateDf.as("df2"),$"df1.Date_Disch" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","DISCHARGE_DATE_SK").drop("Date_Disch")
    val endDateSkAddedDf = dischargeDateSkFactClaimDf.as("df1").join(dimDateDf.as("df2"),$"df1.Date_E" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","END_DATE_SK").drop("Date_E")
    //val dimProviderDf = spark.sql("select * from ncqa_sample.dim_provider")
    val dimProviderDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimProviderTableName)
    val providerSkAddedFactClaimDf = endDateSkAddedDf.as("df1").join(dimProviderDf.as("df2"),$"df1.ProvID" === $"df2.PROV_ID").select($"df1.*",$"df2.PROVIDER_SK").withColumnRenamed("PROVIDER_SK","PROVIDER_SK").drop("ProvID")
    /* End of join Df with different tables to get the sks*/


    /*creating column array that contains the details of newly adding columns*/
    val newColumnArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnsAddedFactClaimDf = UtilFunctions.addingColumnsToDataFrame(providerSkAddedFactClaimDf,newColumnArray)

    /*claims_sk creation*/
    val claimSkAddedFactClaimDf = newlyColumnsAddedFactClaimDf.withColumn("CLAIMS_SK",abs(lit(concat(lit($"CLAIMS_ID"),lit($"CLAIM_LINE_ID"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))
    claimSkAddedFactClaimDf.printSchema()

    /*creating array of column names for ordering the Df*/
    val schemaArray = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*Ordering the Dataframe*/
    val factClaimFormattedDf = claimSkAddedFactClaimDf.select(schemaArray.head,schemaArray.tail:_*)
    factClaimFormattedDf.printSchema()
    //factClaimFormattedDf.show()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factClaimsTableName
    factClaimFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }
}
