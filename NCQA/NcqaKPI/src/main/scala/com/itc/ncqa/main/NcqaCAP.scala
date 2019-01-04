package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{datediff, lit}
import org.apache.spark.sql.types.DateType

object NcqaCAP {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and SaprkSession oBject creation">

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACAP")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Allowable Gap, Age filter,Continuous Enrollment.">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.capMeasureTitle)

    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")


    /*Age filter based on the measure id*/
    val ageFilterDf = measureId match {
      /*CAP1(12 -24 months)*/
      case KpiConstants.cap1MeasureId => {
        var current_date = year + "-12-31"
        val newDf1 = commonFilterDf.withColumn("curr_date", lit(current_date))
        val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))
        newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 30).>=(12) && (datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 30).<=(24)).drop("curr_date")

      }

      /*CAP2(25 months - 6 years)*/
      case KpiConstants.cap2MeasureId =>{
        var current_date = year + "-12-31"
        val newDf1 = commonFilterDf.withColumn("curr_date", lit(current_date))
        val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))
        newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 30).>=(25) && (datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 30).<=(72)).drop("curr_date")

      }

      /*CAP3(7-11 Years)*/
      case KpiConstants.cap3MeasureId => {

        UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age7Val, KpiConstants.age11Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      }

      /*CAP4(12- 19 years)*/
      case KpiConstants.cap4MeasureId => {

        UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age12Val, KpiConstants.age19Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
      }
    }

    /*Continous enrollment calculation*/
    val contEnrollEndDate = year + "-12-31"
    var contEnrollStrtDate = ""

    if(KpiConstants.cap1MeasureId.equalsIgnoreCase(measureId) || KpiConstants.cap2MeasureId.equalsIgnoreCase(measureId)){

      contEnrollStrtDate = year + "-01-01"
    }
    else {

      contEnrollStrtDate = year.toInt-1 + "-01-01"
    }

    val continiousEnrollDf = ageFilterDf.filter(ageFilterDf.col(KpiConstants.memStartDateColName).<(contEnrollStrtDate) &&(ageFilterDf.col(KpiConstants.memEndDateColName).>(contEnrollEndDate)))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = continiousEnrollDf
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Upper measurement limit based on the measure id*/
    var measrUpperValue = 0
    /*For CAP1 and CAP2 (during the year(1))*/
    if(KpiConstants.cap1MeasureId.equalsIgnoreCase(measureId) || KpiConstants.cap2MeasureId.equalsIgnoreCase(measureId)){

      measrUpperValue = KpiConstants.measurement1Val
    }
    /*For CAP1 and CAP2 (during the year and prior to the year(2))*/
    else {

      measrUpperValue = KpiConstants.measurement2Val
    }

    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    /*Ambulatory Visit as Primary Diagnosis*/
    val ambulatoryVisitList = List(KpiConstants.ambulatoryVisitVal)
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForAmbVisAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.capMeasureId,ambulatoryVisitList,primaryDiagCodeSystem)
    val measurForAmbVisAsDiagDf = UtilFunctions.mesurementYearFilter(joinedForAmbVisAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,measrUpperValue).select(KpiConstants.memberskColName)

    /*Ambulatory Visit as Proceedure code*/
    val ambulatoryVisitCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAmbVisAsProDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.capMeasureId,ambulatoryVisitList,ambulatoryVisitCodeSystem)
    val measurForAmbVisAsProDf = UtilFunctions.mesurementYearFilter(joinedForAmbVisAsProDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,measrUpperValue).select(KpiConstants.memberskColName)

    val ambulatoryVistDf = measurForAmbVisAsDiagDf.union(measurForAmbVisAsProDf)

    /*Numerator(members who has in dinominator and an ambulatory visit in the measurement year)*/
    val numeratorDf = ambulatoryVistDf.intersect(dinoForKpiCalDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = ambulatoryVisitList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)


    val numExclDf = spark.emptyDataFrame
    val dinominatorExclDf = spark.emptyDataFrame

    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()

  }
}
