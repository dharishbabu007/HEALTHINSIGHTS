package com.itc.ncqa.main


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.itc.ncqa.Functions.SchemaLoad
import com.itc.ncqa.Functions.DataLoad
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object NcqaMain {

  def main(args: Array[String]): Unit = {

    println("started")
   // System.setProperty("hadoop.home.dir", "D:\\Sangeeth\\hadoop_home")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAPOC")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val inputPath =  args(0)
    val schemaPath = args(1)
    val validOutputPath = args(2)
    val invalidOutputPath = args(3)


    /*loading the schema file from hdfs*/
    val schemaRdd = spark.sparkContext.textFile(schemaPath, 1)
   /*create structtype from schema file*/
    val schemaType = SchemaLoad.schemaCreationFromSchemaRDD(schemaRdd)
   /*creating the schema array for validate each row*/
    val validationArray = SchemaLoad.dataForValidationFromSchemaRdd(schemaRdd)


    /*load the input file from hdfs*/
    val inputRdd = spark.sparkContext.textFile(inputPath)
    /*validating the each roww using the function and creating Row Rdd.*/
    val rowRdd = inputRdd.map(foreachrow => DataLoad.convertRowtoRowRdd(foreachrow, validationArray))
    rowRdd.foreach(f=>println(f))
    /*filter out the valid Row Rdd*/
    val validRowRdd = rowRdd.filter(foreachRdd => !foreachRdd.toString().contains("Error"))
   /*filter out the invalid Row Rdd*/
    val invalidRowRdd = rowRdd.filter(foreachRdd => foreachRdd.toString().contains("Error"))
    /*create the dataframe using valid Rdd and struct type*/
    val dataFrame = spark.createDataFrame(validRowRdd, schemaType)
    dataFrame.show()

   /*create the invalid dataframe using valid Rdd and struct type*/
    val inValidDataframe = spark.createDataFrame(invalidRowRdd, StructType(Array(StructField("data",StringType))))
    /*storing the valid dataframe*/
    dataFrame.write.option("header", "true").mode(SaveMode.Overwrite).option("delimiter", "|").csv(validOutputPath)
    /*storing the invalid dataframe*/
    inValidDataframe.write.mode(SaveMode.Append).text(invalidOutputPath)

  }
}
