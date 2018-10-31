package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}

import scala.collection.JavaConversions._

object NcqaABA {


  def main(args: Array[String]): Unit = {


    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    var data_source =""

    /*define data_source based on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }


    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAABA")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._




    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    //print("counts:"+dimMemberDf.count()+","+factClaimDf.count()+","+factMembershipDf.count())

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,refLobDf,dimFacilityDf,lob_name,KpiConstants.abaMeasureTitle)
    //initialJoinedDf.show(50)

    val view45Df = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    val commonFilterDf = initialJoinedDf.as("df1").join(view45Df.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === view45Df.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(view45Df.col("start_date").isNull).select("df1.*")
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf,KpiConstants.dobColName,year,KpiConstants.abaAgeFilterLower,KpiConstants.abaAgeFilterUpper,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    ageFilterDf.show(50)

    /*loading ref_hedis table*/
   // val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*Dinominator Calculation ((Outpatient Value Set during year or previous year)*/
   /* val hedisJoinedForDinominator =  UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,KpiConstants.abavalueSetForDinominator,KpiConstants.abscodeSystemForDinominator)
    val measurement = UtilFunctions.mesurementYearFilter(hedisJoinedForDinominator,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()
    val dinominatorForOutput = ageFilterDf.as("df1").join(measurement.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.*")
    val dinominator = ageFilterDf.as("df1").join(measurement.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === measurement.col(KpiConstants.memberskColName)).select("df1.member_sk").distinct()*/
    //print("-----------------dinominatorForOutput count is----------------- :"+ dinominatorForOutput.count())


    /*Dinominator Exclusion1 (Pregnancy Value Set during year or previous year)*/
   /* val joinForDinominatorExcl = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,KpiConstants.abavaluesetForDinExcl,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementExcl = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)*/

    /*Dinominator Exclusion2 (Hospice)*/
    /*val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf).select(KpiConstants.memberskColName)
    val unionOfDinoExclsionsDf = measurementExcl.union(hospiceDinoExclDf)
    val dinominatorExcl = ageFilterDf.as("df1").join(unionOfDinoExclsionsDf.as("df2"),$"df1.member_sk" === $"df2.member_sk",KpiConstants.innerJoinType).select("df1.member_sk")
*/
    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    //val finalDinominatorDf = dinominator.except(dinominatorExcl)





    /*Numerator1 Calculation (BMI Value Set for 20 years or older)*/
    /*val joinForNumeratorForBmi = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,KpiConstants.abaNumeratorBmiValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementNumeratorBmi = UtilFunctions.mesurementYearFilter(joinForNumeratorForBmi,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper)
    val ageMoreThan20FilterDf = UtilFunctions.ageFilter(ageFilterDf,KpiConstants.dobColName,year,KpiConstants.abaAge20Lower,KpiConstants.abaAgeFilterUpper,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    val numeratorBmiDf = ageMoreThan20FilterDf.as("df1").join(measurementNumeratorBmi.as("df2"),ageMoreThan20FilterDf.col(KpiConstants.memberskColName) === measurementNumeratorBmi.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.member_sk")
*/

    /*Numerator2 Calculation(BMI Percentile Value Set for age between 18 and 20)*/
    /*val joinForNumeratorForBmiPercentile = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.abaMeasureId,KpiConstants.abaNumeratorBmiPercentileValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementNumeratorBmiPercentile = UtilFunctions.mesurementYearFilter(joinForNumeratorForBmiPercentile,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper)
    val ageBetween18And20FilterDf = UtilFunctions.ageFilter(ageFilterDf,KpiConstants.dobColName,year,KpiConstants.abaAgeFilterLower,KpiConstants.abaAge20Lower,KpiConstants.boolTrueVal,KpiConstants.boolFalseval)
    val numeratorBmiPercentileDf = ageBetween18And20FilterDf.as("df1").join(measurementNumeratorBmiPercentile.as("df2"),ageMoreThan20FilterDf.col(KpiConstants.memberskColName) === measurementNumeratorBmi.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.member_sk")
*/
    /*union of 2 numerator condition*/
    //val abaNumeratorDf = numeratorBmiDf.union(numeratorBmiPercentileDf)
    /*Final Numerator(Elements who are present in dinominator and numerator)*/
    //val abanumeratorFinalDf = abaNumeratorDf.intersect(finalDinominatorDf).select(KpiConstants.memberskColName).distinct()
    //abanumeratorFinalDf.show()

   // val numExclDf = spark.emptyDataFrame




   /* val numeratorValueSet = KpiConstants.abaNumeratorBmiValueSet:::KpiConstants.abaNumeratorBmiPercentileValueSet
    val dinominatorExclValueSet = KpiConstants.abavaluesetForDinExcl
    val numeratorExclValueSet = KpiConstants.emptyList

    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)*/
    //listForOutput.foreach(f=>print(f))

   /* val outFormatDf = UtilFunctions.commonOutputDfCreation(spark,dinominatorForOutput,dinominatorExcl,abanumeratorFinalDf,numExclDf,listForOutput)
    outFormatDf.show()*/
    import spark.implicits._

   /* val lobdetailsData = dinominator.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")
    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,intersect,numerator2Df,dimMemberDf,"ABA")
    formattedOutPutDf.show()*/

  }
}
