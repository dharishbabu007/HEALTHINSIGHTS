package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.OutputCreateConstants
import org.apache.spark.sql.{DataFrame, SparkSession}



object DataLoadFunctions {

  def dataLoadFromGapsHedisTable(spark:SparkSession,qualityMsrSk:String):DataFrame ={

    val source_name = "'"+qualityMsrSk+"'"
    val query = "select * from "+ OutputCreateConstants.dbName+"."+ OutputCreateConstants.gapsInHedisTblName+" where "+OutputCreateConstants.qultyMsrSkColName+" ="+source_name
    val dfinit = spark.sql(query)
    dfinit
  }


  def dimMemberDfLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.memberSkColName+","+OutputCreateConstants.memberIdColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.dimMemberTblName
    val dimMemberDf = spark.sql(queryString)
    dimMemberDf
  }


  def dimLobDetailsLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.lobIdColName+","+OutputCreateConstants.lobNameColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.dimLobTblName
    val refLobDf = spark.sql(queryString)
    refLobDf
  }


  def factMembershipLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.lobIdColName+","+OutputCreateConstants.memberSkColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.factMembershipTblName
    val factMembershipDf = spark.sql(queryString)
    factMembershipDf
  }



}
