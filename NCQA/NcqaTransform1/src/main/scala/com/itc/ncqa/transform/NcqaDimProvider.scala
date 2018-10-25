package com.itc.ncqa.transform

import com.itc.ncqa.utils.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, current_timestamp, lit,abs}

object NcqaDimProvider {

  def main(args: Array[String]): Unit = {
    println("started")


    val config = new SparkConf().setAppName("Ncqa Transform1").setMaster("local[*]")
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    val schemaPath = args(0)

    import spark.implicits._

    val schemaRdd = spark.sparkContext.textFile(schemaPath,1)


    val queryString = "select * from ncqa_intermediate.provider"
    val provider = spark.sql(queryString)
    val remove = provider.first()
    val providerDf = provider.filter(row=> row != remove)


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
    dimProviderFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.dim_provider")
  }
}
