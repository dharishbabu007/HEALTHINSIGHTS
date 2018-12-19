package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

object NcqaFactLabs {

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
    val schemaRdd = spark.sparkContext.textFile(schemaPath,1)


    /*val queryString = "select * from ncqa_intermediate.lab"
    val labDf = spark.sql(queryString)
    val memberDf = spark.sql("select * from ncqa_sample.dim_member")*/
    val labDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.labTableName,kpiName)
    val memberDf = DataLoadFunctions.targetTableLoadFunction(spark,TransformConstants.dimMemberTableName)

    /*join Df with tables for getting the sks*/
    val memeberSkAddedFactLabsDf = labDf.as("df1").join(memberDf.as("df2"),$"df1.memid" === $"df2.MEMBER_ID").select($"df1.*",$"df2.MEMBER_SK").drop("MEM_ID")
    //val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val dateFormattedFactlabsDf = memeberSkAddedFactLabsDf.withColumn("date_s",date_format(memeberSkAddedFactLabsDf.col("date_s"),"dd-MMM-yyyy"))
    val servicedateskaddedFactLabsDf = dateFormattedFactlabsDf.as("df1").join(dimDateDf.as("df2"),$"df1.date_s" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","SERVICE_DATE_SK").drop("date_s")
    /*End of join Df with tables for getting the sks*/


   /*creating column array that contains the details of newly adding columns*/
    val newcolumnarray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnAddedFactLabsDf = UtilFunctions.addingColumnsToDataFrame(servicedateskaddedFactLabsDf,newcolumnarray)

    /*creating Lab_sk using the existing columns*/
    val labsSkAddedFactLabsDf = newlyColumnAddedFactLabsDf.withColumn("LABS_SK",abs(lit(concat(lit($"MEMBER_SK"),lit($"SERVICE_DATE_SK"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))

    /*creating array of column names for ordering the Df*/
    val arrayOfColumns = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*Ordering the Dataframe*/
    val formattedFactLabsDf = labsSkAddedFactLabsDf.select(arrayOfColumns.head,arrayOfColumns.tail:_*)
    formattedFactLabsDf.printSchema()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.factLabsTableName
    formattedFactLabsDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }
}
