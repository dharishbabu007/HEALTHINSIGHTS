package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, expr, lit, when}
import org.apache.spark.sql.types.{DataTypes, IntegerType}

object NcqaFactRxClaims {

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

   /* val queryString = "select * from ncqa_intermediate.Pharmacy"
    val pharmacyClicalDf = spark.sql(queryString)*/
    val pharmacyClicalDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.pharmacyTableName,kpiName)

    //val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
    val dimMemberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)
    val daysSuppliedTypeChangeFactRxClaimsDf = pharmacyClicalDf.withColumn("DAYS_SUPPLIED",$"DAYS_SUPPLIED".cast(DataTypes.IntegerType))

    /*member_sk added*/
    val MemberSkAddedfactRxClaimsDf = daysSuppliedTypeChangeFactRxClaimsDf.as("df1").join(dimMemberDf.as("df2"), $"df1.MemID" === $"df2.member_id").select($"df1.*", $"df2.member_sk").drop("MemID").withColumnRenamed("member_sk", "MEMBER_SK")
    /*enddate_sk added */
    val endDateAddedFactRxClaimsDf = MemberSkAddedfactRxClaimsDf.withColumn("date_E",when($"PrServDate".isNull,current_timestamp()).when($"DAYS_SUPPLIED".isNull,$"PrServDate").otherwise(expr("date_add(PrServDate,DAYS_SUPPLIED)")))

    /*formatting the date columns into dd-mm-yyyy fromat*/
    val dateFormattedfactRxClaimsDf = endDateAddedFactRxClaimsDf.withColumn("PrServDate", date_format(endDateAddedFactRxClaimsDf.col("PrServDate"), "dd-MMM-yyyy")).withColumn("date_E",date_format(endDateAddedFactRxClaimsDf.col("date_E"),"dd-MMM-yyyy"))

    //val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*join the df with dim_date for getting date_sk for different columns*/
    val startdateSkAddedfactRxClaimsDf = dateFormattedfactRxClaimsDf.as("df1").join(dimDateDf.as("df2"), $"df1.PrServDate" === $"df2.calendar_date").select($"df1.*", $"df2.date_sk").drop("PrServDate").withColumnRenamed("date_sk", "START_DATE_SK")
    val endDateSkAddedfactRxClaimsDf = startdateSkAddedfactRxClaimsDf.as("df1").join(dimDateDf.as("df2"), $"df1.date_E" === $"df2.calendar_date").select($"df1.*", $"df2.date_sk").drop("date_E").withColumnRenamed("date_sk", "END_DATE_SK")

    /*creating column array that contains the details of newly adding columns*/
    val newColumnArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newColumnsAddedFactRxClaimDf = UtilFunctions.addingColumnsToDataFrame(endDateSkAddedfactRxClaimsDf,newColumnArray)

    /* creating rx_claim_sk using existing columns*/
    val skCreatedFactRxClaimDf = newColumnsAddedFactRxClaimDf.withColumn("RX_CLAIM_SK",abs(lit(concat(lit($"RX_CLAIM_ID"),lit($"RX_CLAIM_LINE_ID"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))


    /*creating array of column names for ordering the Df*/
    val schemaArray = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)
    /*Ordering the Dataframe*/
    val formattedfactRxClaimDf = skCreatedFactRxClaimDf.select(schemaArray.head,schemaArray.tail:_*)
    formattedfactRxClaimDf.printSchema()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factRxClaimsTableName
    formattedfactRxClaimDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }
}
