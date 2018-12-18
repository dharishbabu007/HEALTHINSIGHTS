package com.itc.ncqa.Functions

import com.itc.ncqa.Constants.OutputCreateConstants
import org.apache.spark.sql.{DataFrame, SparkSession}



object DataLoadFunctions {

  /**
    *
    * @param spark - spark session object
    * @param msriDName - measure id to filter from the target table
    * @param lobName - lobName to filter the target table
    * @param ageLower - Lower age limit to filter the target table
    * @param ageUpper - Upper age limit to filter the target table
    * @return - Dataframe which contains the filtered data from fact_gaps_in_hedis table
    * @usecase - This function is used to fetch data from fact_gaps_in_hedis table based on the filterd condition.
    */
  def dataLoadFromGapsHedisTable(spark:SparkSession,msriDName:String,lobName:String,ageLower:Int,ageUpper:Int):DataFrame ={

    val measure_name = "'"+msriDName+"'"
    var query = ""
    if(OutputCreateConstants.abaMeasureId.equalsIgnoreCase(msriDName)) {
      query = "select * from "+ OutputCreateConstants.dbName+"."+ OutputCreateConstants.gapsInHedisTblName+" where "+OutputCreateConstants.ncqaMeasureIdColName+" ="+measure_name+ " and "+
              OutputCreateConstants.lobColName+" ="+ lobName
    }
    else{
      query = "select * from "+ OutputCreateConstants.dbName+"."+ OutputCreateConstants.gapsInHedisTblName+" where "+OutputCreateConstants.ncqaMeasureIdColName+" ="+measure_name+ " and "+
        OutputCreateConstants.ncqaAgeColName + " between "+ageLower+ " and "+ ageUpper+ " and "+ OutputCreateConstants.lobColName+" ="+ lobName
    }
    val dfinit = spark.sql(query)
    dfinit
  }

  /**
    *
    * @param spark - Spark session Object
    * @return - Dataframe which has dim_member details(member_sk , member_id)
    * @usecase - This function is used to fetch the member_sk and member_id from the dim_member table.
    */
  def dimMemberDfLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.memberSkColName+","+OutputCreateConstants.memberIdColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.dimMemberTblName
    val dimMemberDf = spark.sql(queryString)
    dimMemberDf
  }

  /**
    *
    * @param spark - Spark session Object
    * @return - Dataframe which has ref_lob details(lob_id , lob_name)
    * @usecase - This function is used to fetch the lob_id and lob_name from the ref_lob table.
    */
  def dimLobDetailsLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.lobIdColName+","+OutputCreateConstants.lobNameColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.dimLobTblName
    val refLobDf = spark.sql(queryString)
    refLobDf
  }

  /**
    *
    * @param spark - Spark session Object
    * @return - Dataframe which has fact_membership details(lob_id , member_sk)
    * @usecase - This function is used to fetch the lob_id and member_sk from the fact_membership table.
    */
  def factMembershipLoadFunction(spark:SparkSession):DataFrame={

    val queryString = "select "+OutputCreateConstants.lobIdColName+","+OutputCreateConstants.memberSkColName+" from "+OutputCreateConstants.dbName+"."+OutputCreateConstants.factMembershipTblName
    val factMembershipDf = spark.sql(queryString)
    factMembershipDf
  }

}
