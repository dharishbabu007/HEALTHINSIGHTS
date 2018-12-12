package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

import scala.collection.JavaConversions._

object NcqaABA {


  def main(args: Array[String]): Unit = {


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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAABA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    //print("counts:"+dimMemberDf.count()+","+factClaimDf.count()+","+factMembershipDf.count())

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.abaMeasureTitle)
    //initialJoinedDf.show(50)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year.toInt -1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age74Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)


    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Dinominator Calculation ((Outpatient Value Set during year or previous year)*/
    //val hedisJoinedForDinominator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.abaMeasureId, KpiConstants.abavalueSetForDinominator, KpiConstants.abscodeSystemForDinominator)
    val abaOutPatientValSet = List(KpiConstants.outPatientVal)
    val abaOutPatientCodeSystem = List(KpiConstants.hcpsCodeVal,KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val hedisJoinedForDinominator = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaOutPatientValSet,abaOutPatientCodeSystem)
    //val measurement = UtilFunctions.mesurementYearFilter(hedisJoinedForDinominator, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()
    val measurement = UtilFunctions.measurementYearFilter(hedisJoinedForDinominator,KpiConstants.startDateColName,year,0,2)
    val dinominatorForOutput = ageFilterDf.as("df1").join(measurement.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.member_sk", "df1.product_plan_sk", "df1.quality_measure_sk", "df1.facility_sk")
    val dinominator = ageFilterDf.as("df1").join(measurement.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.member_sk").distinct()
    //dinominator.select(KpiConstants.memberskColName).show()

    /*Dinominator Exclusion1 (Pregnancy Value Set during year or previous year)*/
    val abaPregnancyValSet = List(KpiConstants.pregnancyVal)
    val abaPregnancyCodeSystem = List(KpiConstants.icdCodeVal)
    val joinForPregnancyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaPregnancyValSet,abaPregnancyCodeSystem)
    val measForPregnancyDf = UtilFunctions.measurementYearFilter(joinForPregnancyDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion2 (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion union*/
    val dinominatorExcl = measForPregnancyDf.union(hospiceDinoExclDf)

    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val finalDinominatorDf = dinominator.except(dinominatorExcl)


    /*Numerator1 Calculation (BMI Value Set for 20 years or older)*/
    val abaBmiValSet = List(KpiConstants.bmiVal)
    val abaBmiCodeSystem = List(KpiConstants.icdCodeVal)
    val joinForBmiDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaBmiValSet,abaBmiCodeSystem)
    val measForBmiDf = UtilFunctions.measurementYearFilter(joinForBmiDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val ageMoreThan20FilterDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age20Val, KpiConstants.age74Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    val numeratorBmiDf = ageMoreThan20FilterDf.as("df1").join(measForBmiDf.as("df2"), ageMoreThan20FilterDf.col(KpiConstants.memberskColName) === measForBmiDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")


    /*Numerator2 Calculation(BMI Percentile Value Set for age between 18 and 20)*/
    val abaBmiPercentileValSet = List(KpiConstants.bmiPercentileVal)
    val abaBmiPercentileCodeSystem = List(KpiConstants.icdCodeVal)
    val joinForBmiPercentileDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,abaBmiPercentileValSet,abaBmiPercentileCodeSystem)
    val measForBmiPercentileDf = UtilFunctions.measurementYearFilter(joinForBmiPercentileDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val ageBetween18And20FilterDf = UtilFunctions.ageFilter(ageFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age20Val, KpiConstants.boolTrueVal, KpiConstants.boolFalseval)
    val numeratorBmiPercentileDf = ageBetween18And20FilterDf.as("df1").join(measForBmiPercentileDf.as("df2"), ageMoreThan20FilterDf.col(KpiConstants.memberskColName) === measForBmiPercentileDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*union of 2 numerator condition*/
    val abaNumeratorDf = numeratorBmiDf.union(numeratorBmiPercentileDf)
    /*Final Numerator(Elements who are present in dinominator and numerator)*/
    val abanumeratorFinalDf = abaNumeratorDf.intersect(finalDinominatorDf).select(KpiConstants.memberskColName).distinct()




    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = abaBmiValSet ::: abaBmiPercentileValSet
    val dinominatorExclValueSet = abaPregnancyValSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorForOutput, dinominatorExcl, abanumeratorFinalDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)



    /*Data populating to fact_hedis_qms*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.abaMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    spark.sparkContext.stop()
  }
}
