package com.itc.ncqa.Functions




import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import scala.io.Source
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.NullType
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD




object SchemaLoad {



  /*Function Name:schemaCreationFromSchemaRDD
  * Argument name:schemaRdd, Argument type: RDD[String]
  * ReturnType:StructType
  * Description: parse through the schemaRdd and create a struct type*/
  def schemaCreationFromSchemaRDD(schemaRdd: RDD[String]):StructType={

    val schemaArray:Array[StructField] = new Array[StructField](schemaRdd.count().toInt)

    val dataList  = schemaRdd.toLocalIterator.toList
    var i =0
    for(line <- dataList)
    {
      if(line != null)
      {
        var nameOfColumn = line.split(",")(0)
        var typeString = line.split(",")(3)
        var dataType:DataType = typeString match{

          case "INT"       =>  IntegerType
          case "STRING"  =>  StringType
          case "DATE" => DateType
          case "BOOLEAN"   => BooleanType
          case _           => NullType
        }

        val structField = StructField(nameOfColumn,dataType)
        schemaArray(i) = structField
        i=i+1
      }
    }

    val structType:StructType = StructType(schemaArray)
    structType
  }



  /*Function Name:dataForValidationFromSchemaRdd
 * Argument name:schemaRdd, Argument type: RDD[String]
 * ReturnType:ArrayBuffer[String]
 * Description: parse through the schemaRdd and create an arraybuffer that contains the
 * details of the columns*/
  def dataForValidationFromSchemaRdd(schemRdd:RDD[String])={

    var schemadata = new ArrayBuffer[String]

    val schemaDataList = schemRdd.toLocalIterator.toList
    for(line <- schemaDataList)
    {
      if(line != null)
      {
        schemadata.+=(line)
      }
    }
    schemadata
  }

}
