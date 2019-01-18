package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{add_months, count}

object NcqaNCS {


  def main(args: Array[String]): Unit = {


    //<editor-fold desc="Reading program arguments and spark session Object creation">

    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    val measureId = args(4)
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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQANCS")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.ncsMeasureTitle)

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*doing age filter 15 months old during the measurement year */

    val ageAndGenderFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age16Val, KpiConstants.age20Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
                                            .filter($"${KpiConstants.genderColName}".===("F"))

    /*Continous enrollment checking*/
    val contEnrollEndDate = year + "-12-31"
    val contEnrollStartDate = year + "-01-01"
    val contEnrollDf = ageAndGenderFilterDf.filter(ageAndGenderFilterDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && ageAndGenderFilterDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = contEnrollDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion1  ("Cervical Cancer","HIV","HIV Type 2","Disorders of the Immune System" during 31st december of measurement year)*/

    val ncsDinoExclValSet =List(KpiConstants.cervicalCancerVal,KpiConstants.hivVal,KpiConstants.hivType2Val,KpiConstants.disordersoftheImmuneSystemVal)
    val joinForncsDinoExcl = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.ncsMeasureId, ncsDinoExclValSet, primaryDiagnosisCodeSystem)
    val measrNcsExclDf = UtilFunctions.measurementYearFilter(joinForncsDinoExcl, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
                                      .select(KpiConstants.memberskColName)

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName)
    val dinominatorExclDf = hospiceDf.union(measrNcsExclDf)
    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //dinominatorAfterExclusionDf.show()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator1 Calculation (Cervical Cytology Value Set, HPV Tests Value Set)*/

    val ncsNumeratorValSet = List(KpiConstants.cervicalCytologyVal,KpiConstants.hpvTestsVal)
    val ncsNumeratorCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.loincCodeVal,KpiConstants.ubrevCodeVal)
    val joinForNumeratorDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType, KpiConstants.ncsMeasureId, ncsNumeratorValSet, ncsNumeratorCodeSystem)
    val measForNumeratorDf = UtilFunctions.measurementYearFilter(joinForNumeratorDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                          .select(KpiConstants.memberskColName)

    val numeratorDf =  measForNumeratorDf.intersect(dinominatorAfterExclusionDf)
    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = ncsNumeratorValSet
    val dinominatorExclValueSet = ncsDinoExclValSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()

  }

}
