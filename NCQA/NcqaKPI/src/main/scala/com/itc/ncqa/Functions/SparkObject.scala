package com.itc.ncqa.Functions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkObject {


  def sparkObjectCreation():SparkSession={
    /*creating spark session object*/
    //System.setProperty("hadoop.home.dir", "D:\\Sangeeth\\hadoop_home")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIMA")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }

  val spark = sparkObjectCreation()

  def sparkObjectDestroy()={
    this.spark.sparkContext.stop()
  }
}
