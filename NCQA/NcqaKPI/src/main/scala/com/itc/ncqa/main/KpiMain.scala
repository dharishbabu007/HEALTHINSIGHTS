package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import com.itc.ncqa.Functions.SparkObject._

object KpiMain {
  def main(args: Array[String]): Unit = {



    /*Reading the program arguments*/
    val measureId = args(0)
    val measure_sub_id = args(1)
    val lob_name = args(2)
    val programType = args(3)
    val dbName = args(4)
    val year = args(5)


    var data_source = ""

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)


    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val dimQualityMsrDf = DataLoadFunctions.dimqualityMeasureLoadFunction(spark,KpiConstants.imaMeasureTitle)
    val dimQualityPgmDf = DataLoadFunctions.dimqualityProgramLoadFunction(spark, KpiConstants.hedisPgmname)
    val dimProductPlanDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName,KpiConstants.dimProductTblName,data_source)
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, dimProductPlanDf, factMembershipDf, spark.emptyDataFrame, spark.emptyDataFrame, spark.emptyDataFrame, lob_name, "quality")
  }
}
