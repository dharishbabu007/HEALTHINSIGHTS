package com.itc.ncqa.main

import com.itc.ncqa.constants.NcqaGictlConstants
import com.itc.ncqa.functions.{DataLoadFunction, UtilFunction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NcqaGICTempLoad {

  def main(args: Array[String]): Unit = {


    val year = args(0)
    val measureId = args(1)
    val dbName = args(2)
    val programType = args(3)
    NcqaGictlConstants.setDbName(dbName)
    var data_source = ""

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = NcqaGictlConstants.ncqaDataSource
    }
    else {
      data_source = NcqaGictlConstants.clientDataSource
    }

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaQms Load")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    /*loading data from hedisgapsincare table beased on the measureid*/
    val hedisGapsInCareDf = DataLoadFunction.dataLoadDataFromGapsInCare(spark,measureId)

    //<editor-fold desc="Adding Age Column">

    /*Loading dimmember table*/
    val dimMemberDf = DataLoadFunction.dataLoadFromTargetModel(spark,NcqaGictlConstants.dimMemberTableName,data_source)

    /*Join hedis data with Dimmember to get the dobsk for each data*/
    val joinHedisAndDimMemberDf = hedisGapsInCareDf.as("df1").join(dimMemberDf.as("df2"),hedisGapsInCareDf.col(NcqaGictlConstants.memberskColName) === dimMemberDf.col(NcqaGictlConstants.memberskColName),NcqaGictlConstants.innerJoinType)
                                                                    .select("df1.*",NcqaGictlConstants.dobskColame)
    /*laod the dim date table*/
    val dimDateDf = DataLoadFunction.dimDateLoadFunction(spark)

    /*calling function which will ad age column to the hedisdf*/
    val ageColumnAddedDf = UtilFunction.ageColumnAddingFunction(spark,joinHedisAndDimMemberDf,dimDateDf,year)
    //</editor-fold>

    //<editor-fold desc="Lob Dtails adding">

    /*Loading FcatMembership table*/
    val factMembershipDf = DataLoadFunction.dataLoadFromTargetModel(spark,NcqaGictlConstants.factMembershipTblName,data_source)
    /*Loading reflob table*/
    val refLobDf = DataLoadFunction.dataLoadFromTargetModel(spark,NcqaGictlConstants.refLobTblName,data_source)

    /*Calling function to add lob details columns*/
    val lobDetailsAddedDf = UtilFunction.lobDetailsAddingFunction(spark,ageColumnAddedDf,factMembershipDf,refLobDf)
    //</editor-fold>

    //<editor-fold desc="Formatting And savig output Data Frame">

    /*formatting the dataframe into formatted way*/
    val outputFormattedDf = lobDetailsAddedDf.select(NcqaGictlConstants.outFormattedArray.head, NcqaGictlConstants.outFormattedArray.tail:_*)
    /*saving the dataframe to hive table*/
    outputFormattedDf.write.saveAsTable(NcqaGictlConstants.db_name + "." + NcqaGictlConstants.factHedisGapsInCareTmpTblName)
    //</editor-fold>

  }
}
