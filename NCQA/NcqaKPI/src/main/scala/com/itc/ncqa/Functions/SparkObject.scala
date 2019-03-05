package com.itc.ncqa.Functions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkObject {


  def sparkObjectCreation():SparkSession={

    //System.setProperty("hadoop.home.dir", "D:\\Sangeeth\\hadoop_home")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaProgram")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.executor.memory", "5g")
        .set("spark.driver.memory", "5g")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size","16g")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }

  val spark = sparkObjectCreation()

  def sparkObjectDestroy()={
    this.spark.sparkContext.stop()
  }
}
