package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.OutputCreateConstants
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

object UtilityFunctions {


  def getQualityMeasureTitle(qulityMsrStr:String):String ={


    val qultyMsrTitle = qulityMsrStr match {

      case "ABA" => OutputCreateConstants.abaMeasureTitle

    }

    qultyMsrTitle
  }



  def getQualityMsrSk(spark:SparkSession,qultyMsrTitle:String):String ={

    import spark.implicits._

    val measure_title = "'"+qultyMsrTitle+"'"
    val query = "select quality_measure_sk from "+ OutputCreateConstants.dbName+"."+ OutputCreateConstants.dimQltyMsrTblName+" where measure_title ="+measure_title
    val dimQualityMsrDf = spark.sql(query)
    val qulityMsrSk = dimQualityMsrDf.select("quality_measure_sk").as[String].collectAsList()(0)
    qulityMsrSk
  }


  /**
    *
    * @param spark - spark session object
    * @param gapsInHedisDf - dataframe that contains the data for o/p format
    * @param measVal
    * @return
    */
  def outputCreationFunction(spark:SparkSession,gapsInHedisDf:DataFrame,measVal:String):DataFrame={

    import spark.implicits._
    val emptyDf = spark.emptyDataFrame

    val dimMemberDf = DataLoadFunctions.dimMemberDfLoadFunction(spark)
    val memberiDColAddedDf = gapsInHedisDf.as("df1").join(dimMemberDf.as("df2"),gapsInHedisDf.col(OutputCreateConstants.memberSkColName) === dimMemberDf.col(OutputCreateConstants.memberSkColName),OutputCreateConstants.innerJoinType).select("df1.*","df2.member_id")
    val outColsAddedDf = memberiDColAddedDf.withColumn(OutputCreateConstants.ncqaOutEpopCol,lit(1)).withColumn(OutputCreateConstants.ncqaOutExclCol, when(memberiDColAddedDf.col("in_denominator_exclusion").===(OutputCreateConstants.yesVal),lit(OutputCreateConstants.oneVal)).otherwise(lit(OutputCreateConstants.zeroVal)))
                                           .withColumn(OutputCreateConstants.ncqaOutNumCol,when(memberiDColAddedDf.col("in_numerator").===(OutputCreateConstants.yesVal),lit(OutputCreateConstants.oneVal)).otherwise(lit(OutputCreateConstants.zeroVal))).withColumn(OutputCreateConstants.ncqaOutRexclCol,lit(OutputCreateConstants.zeroVal))
                                           .withColumn(OutputCreateConstants.ncqaOutIndCol,lit(OutputCreateConstants.zeroVal)).withColumn(OutputCreateConstants.ncqaOutMeasureCol,lit(measVal)).withColumnRenamed("member_id",OutputCreateConstants.ncqaOutmemberIdCol).withColumnRenamed(OutputCreateConstants.lobNameColName,OutputCreateConstants.ncqaOutPayerCol)
                                           .select(OutputCreateConstants.ncqaOutmemberIdCol,OutputCreateConstants.ncqaOutEpopCol,OutputCreateConstants.ncqaOutExclCol,OutputCreateConstants.ncqaOutNumCol,OutputCreateConstants.ncqaOutRexclCol,OutputCreateConstants.ncqaOutIndCol,OutputCreateConstants.ncqaOutMeasureCol,OutputCreateConstants.ncqaOutPayerCol)

    //outColsAddedDf.show(50)
    /*val factMembershipDf = DataLoadFunctions.factMembershipLoadFunction(spark)
    val joinedWithFactMembershipDf = outColsAddedDf.as("df1").join(factMembershipDf.as("df2"),outColsAddedDf.col(OutputCreateConstants.memberSkColName) === factMembershipDf.col(OutputCreateConstants.memberSkColName),OutputCreateConstants.innerJoinType).select("df1.*","df2.lob_id")
    val refLobDf = DataLoadFunctions.dimLobDetailsLoadFunction(spark)
    val lobNameColAddedDf = joinedWithFactMembershipDf.as("df1").join(refLobDf.as("df2"),joinedWithFactMembershipDf.col(OutputCreateConstants.lobIdColName) === refLobDf.col(OutputCreateConstants.lobIdColName),OutputCreateConstants.innerJoinType).select("df1.*","df2.lob_name").withColumnRenamed("lob_name",OutputCreateConstants.ncqaOutPayerCol)*/
    val outputFormattedDf = outColsAddedDf.select(OutputCreateConstants.outColsFormatList.head,OutputCreateConstants.outColsFormatList.tail:_*)

    outputFormattedDf.show(50)
    emptyDf
  }

}
