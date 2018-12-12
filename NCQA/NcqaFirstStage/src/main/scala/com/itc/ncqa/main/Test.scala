package com.itc.ncqa.main

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {


    val inputPath = args(0)
    /*creating the spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAFIRSTSTAGE")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df = spark.read.parquet(inputPath)
    df.show()
    df.printSchema()
  }
}
