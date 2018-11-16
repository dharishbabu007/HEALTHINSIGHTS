
package com.itc.ncqa.main
import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import scala.collection.JavaConversions._

object NcqaCHL {



  def main(args: Array[String]): Unit = {

    /*Reading the program arguments*/

    val year = args(0)
    val lob_name = args(1)  /* Need to Check if Lob_id or Lob_name need to be passed */
    val programType = args(2)
    val dbName = args(3)
    var data_source =""
    /*define data_source based on program type. */
    if("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }


    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)


    /*define data_source based on program type. */
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACHL1")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    var lookupTableDf = spark.emptyDataFrame

    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimMemberTblName,data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factClaimTblName,data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.factMembershipTblName,data_source)
    val factRxClaimsDF = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants. factRxClaimTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimFacilityTblName,data_source).select(KpiConstants.facilitySkColName)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName,KpiConstants.dimLocationTblName,data_source)

    /*Initial join function call for prepare the data from common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark,dimMemberDf,factClaimDf,factMembershipDf,dimLocationDf,refLobDf,dimFacilityDf,lob_name,KpiConstants.abaMeasureTitle)

    var lookUpDf = spark.emptyDataFrame

    if(lob_name.equalsIgnoreCase(KpiConstants.commercialLobName)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }


    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*Age filter*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf,KpiConstants.dobColName,year,KpiConstants.age16Val,KpiConstants.age24Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    /*Gender filter*/
    val genderFilterDf =ageFilterDf.filter($"gender".===("F"))


    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)



    /*Dinominator Calculation Starts*/

    /*Sexual Activity,Pregnancy dinominator Calculation*/
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.chlMeasureId,KpiConstants.chlSexualActivityValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val sexualAndPregnancyDistinctDf = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()


    /*Pregnancy Tests Dinominator Calculation*/
    val hedisJoinedForSecondDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType, KpiConstants.chlMeasureId,KpiConstants.chlpregnancyValueSet,KpiConstants.chlpregnancycodeSystem)
    val pregnancyTestMeasurementDf = UtilFunctions.mesurementYearFilter(hedisJoinedForSecondDino,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val pregnancyTestDistinctDf = pregnancyTestMeasurementDf.select("member_sk").distinct()


    /*Pharmacy data Dinominator*/
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val ref_medvaluesetDf = spark.sql("select * from healthin.ref_med_value_set")
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDF.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDF.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"),factRxClaimsDF.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName),KpiConstants.innerJoinType).filter(ref_medvaluesetDf.col(KpiConstants.measureIdColName).===(KpiConstants.chlMeasureId)).select("df1.member_sk","df2.start_date_sk")
    val startDateValAddedDfForThirddDino = medValuesetForThirdDino.as("df1").join(dimDateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForThirdDino = startDateValAddedDfForThirddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val pharmacyDistinctDf = UtilFunctions.mesurementYearFilter(dateTypeDfForThirdDino,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()

    /*union of three different dinominator calculation*/
    val unionOfThreeDinominatorDf = sexualAndPregnancyDistinctDf.union(pregnancyTestDistinctDf).union(pharmacyDistinctDf)
    val dinominatorDf = genderFilterDf.as("df1").join(unionOfThreeDinominatorDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.*")
    val dinominatorForKpiCal = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    /*End of dinominator Calculation */


    /*Dinominator Exclusion Starts*/

    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf).select(KpiConstants.memberskColName)


    /*pregnancy test Exclusion during measurement period*/

    val hedisJoinedForPregnancyExclusion = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType, KpiConstants.chlMeasureId,KpiConstants.chlPregnancyExclusionvalueSet,KpiConstants.chlPregnancyExclusioncodeSystem)
    val measurementDfPregnancyExclusion = UtilFunctions.mesurementYearFilter(hedisJoinedForPregnancyExclusion,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()

    /*isotretinoin filter*/

    val medValuesetForIsoExcl = pregnancyTestMeasurementDf.as("df1").join(factRxClaimsDF.as("df2"),pregnancyTestMeasurementDf.col(KpiConstants.memberskColName) === factRxClaimsDF.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"),factRxClaimsDF.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName),KpiConstants.innerJoinType).filter(ref_medvaluesetDf.col(KpiConstants.measureIdColName).===(KpiConstants.chlMeasureId)).select("df1.*","df2.start_date_sk")
    //medValuesetForIsoExcl.printSchema()
    val startDateValAddedDfForIsoExcl = medValuesetForIsoExcl.as("df1").join(dimDateDf.as("df2"),  medValuesetForIsoExcl.col(KpiConstants.startDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName)).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.startTempColName).drop(KpiConstants.startDateSkColName)
    val dateTypeDfForIsoExcl = startDateValAddedDfForIsoExcl.withColumn(KpiConstants.rxStartTempColName, to_date(startDateValAddedDfForIsoExcl.col(KpiConstants.startTempColName), "dd-MMM-yyyy")).drop(startDateValAddedDfForIsoExcl.col(KpiConstants.startTempColName))
    //dateTypeDfForIsoExcl.printSchema()


    val dayFilteredDfForIsoExcl = dateTypeDfForIsoExcl.filter((datediff(dateTypeDfForIsoExcl.col(KpiConstants.rxStartTempColName), dateTypeDfForIsoExcl.col(KpiConstants.startDateColName)).===(0)) ||  ( datediff(dateTypeDfForIsoExcl.col(KpiConstants.rxStartTempColName), dateTypeDfForIsoExcl.col(KpiConstants.startDateColName)).===(6)))
    /*pregnancyExclusion and isotretinoinDf join*/
    val prgnancyExclusionandIsoDf = measurementDfPregnancyExclusion.as("df1").join(dayFilteredDfForIsoExcl.as("df2"),measurementDfPregnancyExclusion.col(KpiConstants.memberskColName) === dayFilteredDfForIsoExcl.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select(measurementDfPregnancyExclusion.col(KpiConstants.memberskColName)).distinct()


    /*Diagnostic Radiology Value Set Exclusion*/
   // pregnancyTestMeasurementDf.printSchema()


    val hedisJoinedForDigRadExclusion = pregnancyTestMeasurementDf.as("df1").join(factClaimDf.as("df2"), pregnancyTestMeasurementDf.col(KpiConstants.memberskColName) === factClaimDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(refHedisDf.as("df3"),factClaimDf.col(KpiConstants.proceedureCodeColName) === $"df3.code",KpiConstants.innerJoinType).filter($"measureid".===(KpiConstants.chlMeasureId).&&($"valueset".isin(KpiConstants.chlDiagRadValueSet:_*)).&&($"codesystem".like("CPT"))).select("df1.*","df2.start_date_sk")
    //val hedisJoinedForDigRadExclusion = pregnancyTestMeasurementDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", KpiConstants.innerJoinType).join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code", KpiConstants.innerJoinType).filter($"measureid".===(KpiConstants.chlMeasureId).&&($"valueset".isin(KpiConstants.chlDiagRadValueSet:_*)).&&($"codesystem".like("CPT"))).select("df1.*","df2.start_date_sk")
    //val hedisJoinedForDigRadExclusion = pregnancyTestMeasurementDf.as("df1").join(factClaimDf.as("df2"), pregnancyTestMeasurementDf.col("member_sk") === factClaimDf.col("member_sk"), KpiConstants.innerJoinType).join(refHedisDf.as("df3"),$"df2.procedure_code" === $"df3.code", KpiConstants.innerJoinType).filter($"measureid".===(KpiConstants.chlMeasureId).&&($"valueset".isin(KpiConstants.chlDiagRadValueSet:_*)).&&($"codesystem".like("CPT"))).select("df1.*","df2.start_date_sk")
    val startDateValAddedDfDigRadExclusion = hedisJoinedForDigRadExclusion.as("df1").join(dimDateDf.as("df2"), hedisJoinedForDigRadExclusion.col(KpiConstants.startDateSkColName) === dimDateDf.col(KpiConstants.dateSkColName)).select($"df1.*", $"df2.calendar_date").withColumnRenamed(KpiConstants.calenderDateColName, KpiConstants.startTempColName).drop(KpiConstants.startDateSkColName)
    val dateTypeDfDigRadExclusion = startDateValAddedDfDigRadExclusion.withColumn(KpiConstants.diagStartColName, to_date(startDateValAddedDfDigRadExclusion.col(KpiConstants.startTempColName), "dd-MMM-yyyy")).drop( startDateValAddedDfDigRadExclusion.col(KpiConstants.startTempColName))
    val dayFilterdDigRadExclusion = dateTypeDfDigRadExclusion.filter(datediff(dateTypeDfDigRadExclusion.col(KpiConstants.diagStartColName),dateTypeDfDigRadExclusion.col(KpiConstants.startDateColName)).===(0) ||(datediff(dateTypeDfDigRadExclusion.col(KpiConstants.diagStartColName),dateTypeDfDigRadExclusion.col(KpiConstants.startDateColName)).===(6))).select(KpiConstants.memberskColName).distinct()

    /*pregnancyExclusion and Diagnostic Radiology join*/
    val pregnancyExclusionandDigRadDf = measurementDfPregnancyExclusion.as("df1").join(dayFilterdDigRadExclusion.as("df2"),measurementDfPregnancyExclusion.col(KpiConstants.memberskColName) === dayFilterdDigRadExclusion.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select(measurementDfPregnancyExclusion.col(KpiConstants.memberskColName)).distinct()

    /*prgnancyExclusionandIsoDf and pregnancyExclusionandDigRadDf union */
    val unionOfExclusionDf = prgnancyExclusionandIsoDf.union(pregnancyExclusionandDigRadDf).distinct()


    /*join of pregnancy test Df and prgnancyExclusionandIsoDf and pregnancyExclusionandDigRadDf union*/
    val secondDinominatorExclusion = pregnancyTestDistinctDf.as("df1").join(unionOfExclusionDf.as("df2"),pregnancyTestDistinctDf.col(KpiConstants.memberskColName) === unionOfExclusionDf.col(KpiConstants.memberskColName)).select(pregnancyTestDistinctDf.col(KpiConstants.memberskColName)).distinct()

    /*union of hospice and the secondDinominatorExclusion*/
    val dinominatorExclusionDf = hospiceDf.union(secondDinominatorExclusion).distinct()
    /*End of Dinominator Exclusion Starts*/

    /*Final dinominator after Exclusion*/
    val dinominatorFinalDf = dinominatorForKpiCal.except(dinominatorExclusionDf)

    /*Numerator calculation*/

    val hedisJoinedForNumerator = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.chlMeasureId,KpiConstants.chlchalmdiaValueSet,KpiConstants.chlChalmdiacodeSystem)

    val measurementDfForNumerator = UtilFunctions.mesurementYearFilter(hedisJoinedForNumerator,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select("member_sk").distinct()

    val numeratorDistinctDf = measurementDfForNumerator.intersect(dinominatorFinalDf)
    numeratorDistinctDf.show()



    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.chlchalmdiaValueSet
    val dinominatorExclValueSet = KpiConstants.chlPregnancyExclusionvalueSet:::KpiConstants.chlDiagRadValueSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.chlMeasureId)


    val numExclDf = spark.emptyDataFrame

    /*val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclusionDf, numeratorDistinctDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto("ncqa_sample.gaps_in_hedis_test")*/

    spark.sparkContext.stop()
  }



}


















