package com.itc.ncqa.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_format, lit}

import scala.collection.mutable.ArrayBuffer

object UtilFunctions {




  def dataForValidationFromSchemaRdd(schemRdd:RDD[String])={

    var schemadata = new ArrayBuffer[String]

    val schemaDataList = schemRdd.toLocalIterator.toList
    for(line <- schemaDataList)
    {
      if(line != null)
      {
        schemadata.+=(line.split(",")(0))
      }
    }
    schemadata
  }




  def newlyAddedColumnListCreationFromRdd(schemaRdd:RDD[String]):ArrayBuffer[String] ={
  var columnList = new ArrayBuffer[String]()

    val schemaDataList = schemaRdd.toLocalIterator.toList
    for(line <- schemaDataList)
    {
      if((line != null) && (line.split(",")(2).equals("Y")))
      {
        columnList.+=(line)
      }
    }
    columnList
}






  def addingColumnsToDataFrame(dataFrame:DataFrame,columnArray:ArrayBuffer[String])={

    var tempDf = dataFrame

    for(value <- columnArray)
      {
        value.split(",")(1) match {
//
          case "STRING" => if(value.split(",")(3).toLowerCase.equals("null")) {
            tempDf = tempDf.withColumn(value.split(",")(0),lit(""))
          }

          else{
            tempDf = tempDf.withColumn(value.split(",")(0),lit(value.split(",")(3)))
          }

          case "VARCHAR(1)" => if(value.split(",")(3).toLowerCase.equals("null")) {
            tempDf = tempDf.withColumn(value.split(",")(0),lit(""))
          }

          else{
            tempDf = tempDf.withColumn(value.split(",")(0),lit(value.split(",")(3)))
          }

          case "INT" => if (value.split(",")(3).toLowerCase.equals("null")){
            tempDf = tempDf.withColumn(value.split(",")(0),lit(0))
          }
            else{
            tempDf = tempDf.withColumn(value.split(",")(0),lit(value.split(",")(3).toInt))
          }
          case "FLOAT" => if (value.split(",")(3).toLowerCase.equals("null")){
            tempDf = tempDf.withColumn(value.split(",")(0),lit(0.0))
          }
          else{
            tempDf = tempDf.withColumn(value.split(",")(0),lit(value.split(",")(3).toFloat))
          }
          case "TIMESTAMP" => if (value.split(",")(3).toLowerCase.equals("null")){
            tempDf = tempDf.withColumn(value.split(",")(0),lit(""))
          }
          else{
            tempDf = tempDf.withColumn(value.split(",")(0),lit(current_timestamp))
          }
        }
      }
    tempDf
  }

}
