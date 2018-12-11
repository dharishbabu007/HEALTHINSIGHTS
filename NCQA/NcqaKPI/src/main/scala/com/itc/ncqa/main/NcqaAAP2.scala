package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}


object NcqaAAP2 {


  def main(args: Array[String]): Unit = {

    /*Reading the program arguments*/

    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source =""
    var measurementYearUpper = 0

    /*define data_source on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*define Lob_Name  is commercial three years as mesurement year else one year*/
    if("Commercial".equals(lob_name)) {
<<<<<<< HEAD
      measurementYearUpper = KpiConstants.measurementThreeyearUpper
=======
      measurementYearUpper = KpiConstants.measurementThreeYearUpper
>>>>>>> origin/sangeeth
    }
    else {
      measurementYearUpper = KpiConstants.measurementOneyearUpper
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)


    /*define data_source based on program type. */
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAAAP2")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val ref_lobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,ref_lobDf,dimFacilityDf,lob_name,KpiConstants.lscMeasureTitle)


    var lookUpDf = spark.emptyDataFrame

    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }


    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*Age 45–64 (Aap1 Activity)*/

<<<<<<< HEAD
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age45val, KpiConstants.age64val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
=======
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age45Val, KpiConstants.age64Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
>>>>>>> origin/sangeeth



    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)


    /*find out the hospice members*/

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()


    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()

    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)

    //dinominatorAfterExclusionDf.show()

    /*Numerator Calculation*/

    /*Numerator1 Calculation (Adults’ Access to Preventive/Ambulatory Health Services (AAP) )*/

    /*Numerator1 Calculation for primaryDiagnosisCodeSystem */
    val joinForNumeratorForaap21 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.aap2MeasureId, KpiConstants.aapValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementNumeratoraap21 = UtilFunctions.mesurementYearFilter(joinForNumeratorForaap21, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, measurementYearUpper)
    val numeratoraap2Df1 = ageFilterDf.as("df1").join(measurementNumeratoraap21.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratoraap21.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*Numerator1 Calculation  for proceedureCodeSystem */
    val joinForNumeratorForaap22 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.aap2MeasureId, KpiConstants.aapValueSet, KpiConstants.aapCodeSystem)
    val measurementNumeratoraap22 = UtilFunctions.mesurementYearFilter(joinForNumeratorForaap22, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, measurementYearUpper)
    val numeratoraap2Df2 = ageFilterDf.as("df1").join(measurementNumeratoraap22.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratoraap22.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    val numeratoraap2Df = numeratoraap2Df1.union(numeratoraap2Df2)

    /*Final Numerator(Elements who are present in dinominator and numerator)*/
    val aap2numeratorFinalDf = numeratoraap2Df.intersect(dinominatorAfterExclusionDf).select(KpiConstants.memberskColName).distinct()

    aap2numeratorFinalDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.aapValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.aap2MeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, hospiceDf, aap2numeratorFinalDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Aapend).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)



    /*Data populating to fact_hedis_qms*/
    //   val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.WCCMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    //   val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    //   val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //  outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    spark.sparkContext.stop()
  }



}
