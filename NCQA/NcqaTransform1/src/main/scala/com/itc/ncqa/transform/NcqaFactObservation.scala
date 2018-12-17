package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

object NcqaFactObservation {

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


    /*val queryString = "select * from ncqa_intermediate.observation"
    val observationDf = spark.sql(queryString)*/
    val observationDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.observationTableName,kpiName)
    observationDf.show()
    //val dimMemberDf = spark.sql("select * from ncqa_sample.dim_member")
    val dimMemberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)
    /*patient_sk adding from dim_member table*/
    val patientskAddedFactObservationDf = observationDf.as("df1").join(dimMemberDf.as("df2"),$"df1.ptid" === $"df2.member_id").select($"df1.*",$"df2.member_sk").withColumnRenamed("member_sk","PATIENT_SK").drop("ptid")
    val dateFormattedFactObservationDf = patientskAddedFactObservationDf.withColumn("date_o",date_format(patientskAddedFactObservationDf.col("date_o"),"dd-MMM-yyyy")).withColumn("date_e",date_format(patientskAddedFactObservationDf.col("date_e"),"dd-MMM-yyyy"))
    //dateFormattedFactObservationDf.show()
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val observationdateskaddedFactObservationDf = dateFormattedFactObservationDf.as("df1").join(dimdateDf.as("df2"),$"df1.date_o" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","OBSERVATION_DATE_SK").drop("date_o")
    //observationdateskaddedFactObservationDf.show()
    val enddateskaddedFactObservationDf = observationdateskaddedFactObservationDf.as("df1").join(dimdateDf.as("df2"),$"df1.date_e" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","END_DATE_SK").drop("date_e")
    //enddateskaddedFactObservationDf.show()

    /*creating column array that contains the details of newly adding columns*/
    val newColumnArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnAddedFactObservationDf = UtilFunctions.addingColumnsToDataFrame(enddateskaddedFactObservationDf,newColumnArray)

    /*creating observation_sk using existing columns*/
    val observationskAddedFactObservationDf = newlyColumnAddedFactObservationDf.withColumn("OBSERVATION_SK",abs(lit(concat($"PATIENT_SK",lit($"OBSERVATION_DATE_SK"),lit($"END_DATE_SK"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))

    /*creating array of column names for ordering the Df*/
    val arrayOfColumns = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*Ordering the Dataframe*/
    val formattedFactObservationDf = observationskAddedFactObservationDf.select(arrayOfColumns.head,arrayOfColumns.tail:_*)
    //formattedFactObservationDf.show()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factObservationTableName
    formattedFactObservationDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }
}
