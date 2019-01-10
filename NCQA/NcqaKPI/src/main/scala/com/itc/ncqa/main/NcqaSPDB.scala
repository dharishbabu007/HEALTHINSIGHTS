package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object NcqaSPDB {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading Program arguments and SparkSession Object creation">

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASPDB")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required tables to memory">

    import spark.implicits._
    var lookupTableDf = spark.emptyDataFrame

    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Continoius Enrollment,Allowable Gap and Age filter.">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spdMeasureTitle)

    /*Continuous Enrollment Checking*/
    val contEnrollStartDate = year.toInt - 1 + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val continuousEnrollDf = initialJoinedDf.filter(initialJoinedDf.col(KpiConstants.memStartDateColName).<=(contEnrollStartDate) && initialJoinedDf.col(KpiConstants.memEndDateColName).>=(contEnrollEndDate))

    /*Loading view table based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = continuousEnrollDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 40 and 75*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*Dinominator Calculation Starts*/
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
    /*High and Moderate-Intensity Statin Medications List; Low-Intensity Statin Medications List*/
    val hmiliStatinValList = List(KpiConstants.highAndModerateStatinMedVal, KpiConstants.lowStatinMedVal)
    val joinedForHmismDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark, factRxClaimsDf, ref_medvaluesetDf, KpiConstants.spdaMeasureId,hmiliStatinValList)
    val measurementForHmismDf = UtilFunctions.measurementYearFilter(joinedForHmismDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
    val dinoDf = measurementForHmismDf.select(KpiConstants.memberskColName)
    val dinominatorDf = ageFilterDf.as("df1").join(dinoDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinoDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    /*Dinominator Calculation Ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion starts*/
    val primaryDiagnosisCodeSystem = List(KpiConstants.icdCodeVal)
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*Dinominator Exclusion1(Hospice Exclusion)*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)

    /*Find out the members who are not in diabetes valueset*/
    val diabtesValList = List(KpiConstants.diabetesVal)
    val joinedForDiabetesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, diabtesValList,primaryDiagnosisCodeSystem)
    val measurementForDiab1Df = UtilFunctions.measurementYearFilter(joinedForDiabetesDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    val membersWithoutDiabetesDf = dimMemberDf.select(KpiConstants.memberskColName).except(measurementForDiab1Df)

    /*Members who has diabetes exclusion valueset*/
    val diabExclValList = List(KpiConstants.diabetesExclusionVal)
    val hedisJoinedForDiabetesExclDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, diabExclValList, primaryDiagnosisCodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.measurementYearFilter(hedisJoinedForDiabetesExclDf, KpiConstants.startDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Dinominator Exclusion2(Members who do not have a diagnosis of diabetes (Diabetes Value Set) and have (Diabetes Exclusions Value Set))*/
    val dinoExclusion2Df = membersWithoutDiabetesDf.intersect(measurementDiabetesExclDf)

    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName).union(dinoExclusion2Df)
    //dinominatorExclDf.printSchema()
    val dinoAfterExclDf = dinoForKpiCalDf.except(dinominatorExclDf)
    /*Dinominator Exclusion ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator Calculation starts*/
    /*Step1(Find out the IPSD for each member)*/
    val ipsdDf = measurementForHmismDf.select("*").groupBy(KpiConstants.memberskColName).agg(min(measurementForHmismDf.col(KpiConstants.startDateColName)).alias(KpiConstants.ipsdDateColName))
    //ipsdDf.printSchema()

    /*Step2(Find out the treatment period for each member)*/
    var current_date = year + "-12-31"
    val currDateAddedDf = ipsdDf.withColumn("curr_date", lit(current_date))
    val treatmentDaysAddedDf = currDateAddedDf.withColumn(KpiConstants.treatmentDaysColName,datediff(currDateAddedDf.col("curr_date"),currDateAddedDf.col(KpiConstants.ipsdDateColName)))
    //treatmentDaysAddedDf.printSchema()


    /*step3(Total Days Covered by a Statin Medication in the Treatment Period)*/
    val endDateStrtDateDiffDf = measurementForHmismDf.withColumn(KpiConstants.endstrtDiffColName,datediff(measurementForHmismDf.col(KpiConstants.endDateColName),measurementForHmismDf.col(KpiConstants.startDateColName)))
    val sumOfDaysOfStatinDf = endDateStrtDateDiffDf.groupBy(KpiConstants.memberskColName).agg(sum(endDateStrtDateDiffDf.col(KpiConstants.endstrtDiffColName)).alias(KpiConstants.totalStatinDayColName))
    //sumOfDaysOfStatinDf.printSchema()

    /*step4(Find out the PDC using ((totalDays_statinMed/teratment_days)*100))*/
    val joinedForPdcDf = treatmentDaysAddedDf.as("df1").join(sumOfDaysOfStatinDf.as("df2"),treatmentDaysAddedDf.col(KpiConstants.memberskColName) === sumOfDaysOfStatinDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select(treatmentDaysAddedDf.col(KpiConstants.memberskColName),treatmentDaysAddedDf.col(KpiConstants.treatmentDaysColName),sumOfDaysOfStatinDf.col(KpiConstants.totalStatinDayColName))
    val pdcAddedDf = joinedForPdcDf.withColumn(KpiConstants.pdcColName,(joinedForPdcDf.col(KpiConstants.totalStatinDayColName) /(joinedForPdcDf.col(KpiConstants.treatmentDaysColName))).*(100))
    //pdcAddedDf.printSchema()

    /*Step5(find out the members who has pdc >80%)*/
    val pdcMoreThan80Df = pdcAddedDf.filter(pdcAddedDf.col(KpiConstants.pdcColName).>(80)).select(KpiConstants.memberskColName)
    val numeratorDf = pdcMoreThan80Df.intersect(dinoAfterExclDf)
    //pdcMoreThan80Df.count()
    /*Numerator Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care) starts*/
    /*create the reason valueset for output data*/
    val numeratorValueSet = KpiConstants.emptyList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    /*Common output format (data to fact_hedis_gaps_in_care) ends*/
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
