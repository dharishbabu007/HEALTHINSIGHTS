package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object NcqaSPDA {

  def main(args: Array[String]): Unit = {




    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQASPDA")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


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


    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.spdMeasureTitle)

    /*Loading view table based on the lob_name*/
    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }

    /*Remove the Elements who are present on the view table.*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*filter out the members whoose age is between 40 and 75*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age75Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)





    //<editor-fold desc="Dinominator Calculation">
    /*Dinominator starts*/

    /*Loading refhedis data*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    /*Accute Inpatient*/
    val joinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdaAcuteInpatientValueSet,KpiConstants.spdaAcuteInpatientCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.mesurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)


    /*Diabetes Valueset*/
    val joinedForDiabetesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.spdMeasureId, KpiConstants.cdcDiabetesvalueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementForDiab1Df = UtilFunctions.mesurementYearFilter(joinedForDiabetesDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)



    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"measure_id".===(KpiConstants.spdMeasureId)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    val diabetesValuesetDf = measurementForDiab1Df.union(MeasurementForSecondDinoDf)


    /*Telehealth Modifier valueset*/
    val joinedForTeleHealthDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdaTeleHealthValueSet,KpiConstants.spdaTeleHealthCodeSystem)
    val measurementForTeleHealthDf = UtilFunctions.mesurementYearFilter(joinedForTeleHealthDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)

    /*Acute Inpatient Value Set with a Diabetes Value Set and without Telehealth Modifier Value Set; Telehealth POS Value Set*/
    val dinominator1Df = measurementAcuteInpatDf.intersect(diabetesValuesetDf).except(measurementForTeleHealthDf)



    /*at least 2 Outpatient visit*/
    val joinedForTwoOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdOutPatientValueSet,KpiConstants.spdOutPatientCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.mesurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val joinedForTwoObservationDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdObservationValueSet,KpiConstants.spdObservationCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.mesurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val joinedForTwoEdVisistsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdEdVisitValueSet,KpiConstants.spdEdVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.mesurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient*/
    val joinedForTwoNonAcutePatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdNonAcutePatValueSet,KpiConstants.spdNonAcutePatCodeSystem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.mesurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    val dinominator2UnionDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)
    /*atleast 2 visit with Outpatient  or Observation  or ED or Nonacute Inpatient  and has Diabetes */
    val dinominator2Df = dinominator2UnionDf.intersect(diabetesValuesetDf)
    //dinominator2Df.show()




    /*Telephone visit valueset*/
    val joinedForTelephoneVisitDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdTelephoneVisitValueSet,KpiConstants.spdTelephoneVisitCodeSystem)
    val mesrForTelephoneVisitDf = UtilFunctions.mesurementYearFilter(joinedForTelephoneVisitDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val telephoneVisitAndDiabetesDf =  mesrForTelephoneVisitDf.intersect(diabetesValuesetDf)

    /*Online Assesment valueset*/
    val joinedForOnlineAssesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.spdMeasureId,KpiConstants.spdOnlineAssesValueSet,KpiConstants.spdOnlineAssesCodeSystem)
    val mesrForOnlineAssesDf = UtilFunctions.mesurementYearFilter(joinedForOnlineAssesDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    val onlineAssesAndDiabetesDf = mesrForOnlineAssesDf.intersect(diabetesValuesetDf)
    /*Dinominator*/
    val dinominatorDf = dinominator1Df.union(dinominator2Df).union(telephoneVisitAndDiabetesDf).union(onlineAssesAndDiabetesDf)
    /*Dinominator ends*/
    //</editor-fold>



    /*Dinominator Exclusion starts*/
    
    /*Dinominator Exclusion ends*/


  }
}
