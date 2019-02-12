package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DateType
import com.itc.ncqa.Functions.SparkObject._


import scala.collection.JavaConversions._

object Test {

  case class employee(id:String,gender:String,designation:String,state:String)
  case class Member(member_sk:String, start_date:DateType)
  def main(args: Array[String]): Unit = {



    /*val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC2")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()*/

   /* val e1 = employee("1","M","d1","kerala")
   // val e2 = employee("2","M","d2","karnataka")
    val e3 = employee("3","F","d3","Bengal")
    val e4 = employee("4","F","d4","kerala")
    val e5 = employee("5","M","d5","Andhra")
    val e6 = employee("6","F","d6","MP")
    val e7 = employee("7","M","d7","UP")
    val e8 = employee("8","F","d8","TN")
    val e2 = employee("2","M","d2","karnataka")
    val e10 = employee("10","M","d10","Delhi")*/
    import spark.sqlContext.implicits._

   /* //val list = List(e1,e2,e3,e4,e5,e6,e7,e8)
    val list = List(e1,e3,e4,e5,e6,e2,e7,e8,e10)

    val df  = spark.sparkContext.parallelize(list).toDF("id","gender","designation","state")*/
    /*df.show()
    df.printSchema()*/
    //df.write.mode(SaveMode.Append).partitionBy("gender","state").parquet("/home/hbase/ncqa/test/employee/")
    //df.write.mode(SaveMode.Append).partitionBy("gender").bucketBy(2,"designation").parquet("/home/hbase/ncqa/test/employee_wtb/")
    //df.select("id","designation","gender","state").write.format("parquet").mode(SaveMode.Append).insertInto("ncqa_sample.employee_test")

   /* val newDf = spark.read.parquet("/home/hbase/ncqa/test/employee_wtb/")
    newDf.show()*/


   /* val programType = args(0)
    var data_source =""
    /*define data_source based on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }
    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables*/



    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val qualityMeasureSk =  DataLoadFunctions.qualityMeasureLoadFunction(spark,KpiConstants.abaMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk","lob_id")
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark,factMembershipDfForoutDf,qualityMeasureSk,data_source)
    outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")*/




    val inDf = spark.read.csv("D:\\Sangeeth\\NCQA\\Requirement doc\\Ncqa_new\\input.csv")
    inDf.show()

  }
}
