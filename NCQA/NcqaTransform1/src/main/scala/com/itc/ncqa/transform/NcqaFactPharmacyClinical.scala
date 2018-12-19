package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

object NcqaFactPharmacyClinical {

  def main(args: Array[String]): Unit = {

    val schemaFilePath = args(0)
    val sourceDbName = args(1)
    val targetDbName = args(2)
    val kpiName = args(3)
    TransformConstants.setSourceDbName(sourceDbName)
    TransformConstants.setTargetDbName(targetDbName)

    val config = new SparkConf().setAppName("Ncqa FactMembershipCreation").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()

    import spark.implicits._
    val schemaRdd = spark.sparkContext.textFile(schemaFilePath,1)

   /* val queryString = "select * from ncqa_intermediate.pharmacy_clinical"
    val pharmacyClicalDf = spark.sql(queryString)*/
    val pharmacyClicalDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.pharmacyClinicalTableName,kpiName)



    //val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
    val dimMemberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)
    val patientskAddedFactPharmacyClinicalDf = pharmacyClicalDf.as("df1").join(dimMemberDf.as("df2"),$"df1.PtID" === $"df2.member_id").select($"df1.*",$"df2.member_sk").withColumnRenamed("member_sk","PATIENT_SK").drop("PtID")


    val dateformattedFactPharmacyClinicalDf = patientskAddedFactPharmacyClinicalDf.withColumn("ODate",date_format(patientskAddedFactPharmacyClinicalDf.col("ODate"),"dd-MMM-yyyy")).withColumn("StartDate",date_format(patientskAddedFactPharmacyClinicalDf.col("StartDate"),"dd-MMM-yyyy")).withColumn("DDate",date_format(patientskAddedFactPharmacyClinicalDf.col("DDate"),"dd-MMM-yyyy")).withColumn("EDate",date_format(patientskAddedFactPharmacyClinicalDf.col("EDate"),"dd-MMM-yyyy"))

    /*join df with dim_date for getting  date_sk for different columns*/
    //val dimdateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val orderedDateAddedFactPharmacyClinicalDf = dateformattedFactPharmacyClinicalDf.as("df1").join(dimdateDf.as("df2"),$"df1.ODate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","ORDERED_DATE_SK").drop("ODate")
    val startDateAddedFactPharmacyClinicalDf = orderedDateAddedFactPharmacyClinicalDf.as("df1").join(dimdateDf.as("df2"),$"df1.StartDate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","START_DATE_SK").drop("StartDate")
    val dispensedDateAddedFactPharmacyClinicalDf = startDateAddedFactPharmacyClinicalDf.as("df1").join(dimdateDf.as("df2"),$"df1.DDate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","DISPENSED_DATE_SK").drop("DDate")
    val endDateAddedFactPharmacyClinicalDf = dispensedDateAddedFactPharmacyClinicalDf.as("df1").join(dimdateDf.as("df2"),$"df1.EDate" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","END_DATE_SK").drop("EDate")


    /*creating column array that contains the details of newly adding columns*/
    val newColumnArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnAddedFactPharmacyClinicalDf = UtilFunctions.addingColumnsToDataFrame(endDateAddedFactPharmacyClinicalDf,newColumnArray)

    /*creating pharmacyclinical_sk using existing columns*/
    val skkAddedFactPharmacyClinicalDf = newlyColumnAddedFactPharmacyClinicalDf.withColumn("PHARMACY_CLINICAL_SK",abs(lit(concat(lit($"PATIENT_SK"),lit($"ORDERED_DATE_SK"),lit($"START_DATE_SK"),lit($"END_DATE_SK"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))

    /*creating array of column names for ordering the Df*/
    val arrayOfColumns = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*Ordering the Dataframe*/
    val formattedFactPharmacyClinicalDf = skkAddedFactPharmacyClinicalDf.select(arrayOfColumns.head,arrayOfColumns.tail:_*)
    formattedFactPharmacyClinicalDf.printSchema()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factPharmacyClinTableName
    formattedFactPharmacyClinicalDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)


  }
}
