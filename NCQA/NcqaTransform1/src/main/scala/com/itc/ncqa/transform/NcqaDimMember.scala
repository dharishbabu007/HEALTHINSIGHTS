package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object NcqaDimMember {

  def main(args: Array[String]): Unit = {
    println("started")

    val schemaFilePath = args(0)
    val sourceDbName = args(1)
    val targetDbName = args(2)
    val kpiName = args(3)
    TransformConstants.setSourceDbName(sourceDbName)
    TransformConstants.setTargetDbName(targetDbName)

    val config = new SparkConf().setAppName("Ncqa Transform1").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()



    import spark.implicits._
    val schemaRdd = spark.sparkContext.textFile(schemaFilePath,1)

   /* val queryString = "select * from ncqa_intermediate.general_membership"
    val generalMembershipDf = spark.sql(queryString)*/
   val generalMembershipDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.generalMembershipTableName,kpiName)

    generalMembershipDf.show()

    //columnarArray.foreach(println)

    /*load data from dim_date table*/
    //val dimDateDf = spark.sql("select * from ncqa_sample.dim_date")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)

    /*for matting the date column in dd-mm-yyyy*/
    val generalMembershipDf1 = generalMembershipDf.withColumn("DOB",date_format(generalMembershipDf.col("DOB"),"dd-MMM-yyyy"))

    /*adding DATE_OF_BIRTH_SK by join */
    val dobSkAddedDf = generalMembershipDf1.as("df1").join(dimDateDf.as("df2"),$"df1.DOB" === $"df2.calendar_date").select($"df1.*",$"df2.date_sk").withColumnRenamed("date_sk","DATE_OF_BIRTH_SK").drop("DOB")

    /*creating column array that contains the details of newly adding columns*/
    val columnarArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyAddColumnDimMemberDf = UtilFunctions.addingColumnsToDataFrame(dobSkAddedDf,columnarArray)

    /*creating the Member_sk using the existing columns*/
    val memberSkAddedDimMemberDf = newlyAddColumnDimMemberDf.withColumn("MEMBER_SK",abs(lit( concat(lit($"MEMBER_ID"),lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))


    /*creating array of column names for ordering the Df*/
    val arrayOfColumns1 = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)
    //arrayOfColumns1.foreach(println)

    /*Ordering the Dataframe*/
    val formattedDimMemberDf = memberSkAddedDimMemberDf.select(arrayOfColumns1.head,arrayOfColumns1.tail:_*)
    formattedDimMemberDf.show()
    //formattedDf.show()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.dimMemberTableName
    formattedDimMemberDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }
}
