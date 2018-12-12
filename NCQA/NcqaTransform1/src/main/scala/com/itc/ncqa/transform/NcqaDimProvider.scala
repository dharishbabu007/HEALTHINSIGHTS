package com.itc.ncqa.transform

import com.itc.ncqa.constants.TransformConstants
import com.itc.ncqa.utils.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, lit}

object NcqaDimProvider {

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


    /*val queryString = "select * from ncqa_intermediate.provider"
    val provider = spark.sql(queryString)
    val remove = provider.first()
    val providerDf = provider.filter(row=> row != remove)*/
    val providerDf = DataLoadFunctions.sourceTableLoadFunction(spark,TransformConstants.providerTableName,kpiName)


    /*creating column array that contains the details of newly adding columns*/
    val newColumnArray = UtilFunctions.newlyAddedColumnListCreationFromRdd(schemaRdd)

    /*adding new columns to the df*/
    val newlyColumnAddedDimProviderDf = UtilFunctions.addingColumnsToDataFrame(providerDf,newColumnArray)

    /*creating provider_sk using the existing columns*/
    val providerSkAddedDimProviderDf = newlyColumnAddedDimProviderDf.withColumn("PROVIDER_SK",abs(lit(concat($"PROV_ID",lit($"LATEST_FLAG"),lit($"REC_UPDATE_DATE")).hashCode())))

    /*creating array of column names for ordering the Df*/
    val schemaArray = UtilFunctions.dataForValidationFromSchemaRdd(schemaRdd)

    /*Ordering the Dataframe*/
    val dimProviderFormattedDf = providerSkAddedDimProviderDf.select(schemaArray.head,schemaArray.tail:_*)
    dimProviderFormattedDf.printSchema()
    val tableName = TransformConstants.targetDbName + "."+ TransformConstants.dimProviderTableName
    dimProviderFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }
}
