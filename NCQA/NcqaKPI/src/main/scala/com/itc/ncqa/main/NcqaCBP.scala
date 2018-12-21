package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object NcqaCBP {

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

    /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaCBP")
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
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)


    //print("counts:"+dimMemberDf.count()+","+factClaimDf.count()+","+factMembershipDf.count())


    /*Initial join function call to prepare the data for common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.WCCMeasureTitle)
    //initialJoinedDf.show(50)

    var lookUpDf = spark.emptyDataFrame

    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }


    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col(KpiConstants.startDateColName).isNull).select("df1.*")

    /*Age 18–85 */

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age85Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)


    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)

    /*Dinominator calculation*/

    // val dinominatorDf = ageFilterDf.select("df1.member_sk")

    /*Dinominator1a Calculation (for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"  )*/


    val joinForDinominator1a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpCommonDinominatorValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDinominator1a = UtilFunctions.mesurementYearFilter(joinForDinominator1a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val dinominator1aDf = ageFilterDf.as("df1").join(measurementDinominator1a.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementDinominator1a.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*Dinominator1b Calculation (for proceedureCodeSystem with valueset "Outpatient","Telehealth Modifier" combination )*/

    val joinForDinominator1b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominator1ValueSet, KpiConstants.cbpDinominator1CodeSystem)
    val measurementDinominator1b = UtilFunctions.mesurementYearFilter(joinForDinominator1b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val dinominator1bDf = ageFilterDf.as("df1").join(measurementDinominator1b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementDinominator1b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    val dinominator1Df = dinominator1aDf.union(dinominator1bDf)


    /*Dinominator2b Calculation (for proceedureCodeSystem with valueset "Telephone Visits" )*/

    val joinForDinominator2b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominator2ValueSet, KpiConstants.cbpDinominator2CodeSystem)
    val measurementDinominator2b = UtilFunctions.mesurementYearFilter(joinForDinominator2b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val dinominator2bDf = ageFilterDf.as("df1").join(measurementDinominator2b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementDinominator2b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*same as dinominator1 for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"*/

    val dinominator2Df = dinominator1aDf.union(dinominator2bDf)


    /*Dinominator3b Calculation (for proceedureCodeSystem with valueset "Online Assessments" )*/

    val joinForDinominator3b = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominator3ValueSet, KpiConstants.cbpDinominator3CodeSystem)
    val measurementDinominator3b = UtilFunctions.mesurementYearFilter(joinForDinominator3b, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper)
    val dinominator3bDf = ageFilterDf.as("df1").join(measurementDinominator3b.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementDinominator3b.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk")

    /*same as dinominator1 for primaryDiagnosisCodeSystem with valueset "Essential Hypertension"*/

    val dinominator3Df = dinominator1aDf.union(dinominator3bDf)

    val finalDinominatorBeforeExclDf = dinominator1Df.union(dinominator2Df).union(dinominator3Df)


    /*Dinominator Exclusion calculation*/

    /*Dinominator Exclusion for SNP (I-SNP)  and LTI is pending*/

    /*Dinominator Exclusion1 Calculation (for primaryDiagnosisCodeSystem with valueset "Frailty"  )*/

    val joinForDinominator1 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinoExclValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDinominator1 = UtilFunctions.mesurementYearFilter(joinForDinominator1, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("df1.member_sk")

    /*Dinominator Exclusion1a Calculation (for proceedureCodeSystem with valueset "Frailty" )*/

    val joinForDinominatorExcl1a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinoExclValueSet, KpiConstants.cbpDinoExclCodeSystem)
    val measurementDinominatorExcl1a = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl1a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("df1.member_sk")

    val measurementExcl1 = measurementDinominator1.union(measurementDinominatorExcl1a).select("df1.member_sk")

    val ageFilterDfForDinoExcl1 = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age81Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).select("df1.member_sk")

    val dinominatorExcl1 = measurementExcl1.intersect(ageFilterDfForDinoExcl1) /* Age between 81 and above */

    /*Dinominator Exclusion2 Calculation (for primaryDiagnosisCodeSystem with valueset "Advanced Illness"  )*/

    val joinForDinominatorExcl2 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominatorICDExcl2ValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDinominatorExcl2 = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl2, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select("df1.member_sk")

    /*Dinominator Exclusion2a Calculation (for proceedureCodeSystem with valueset "Outpatient","Observation","ED","Nonacute Inpatient" )*/

    val joinForDinominatorExcl2a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominatorExcl2aValueSet, KpiConstants.cbpDinominatorExcl2aCodeSystem)
    val measurementDinominatorExcl2a = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl2a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName, KpiConstants.startDateColName).distinct()

    /*Atleast two of the above valueset occurrence*/
    val measurementDinominator2aCountDf = measurementDinominatorExcl2a.groupBy("member_sk").agg(count("start_date").alias("count1")).filter($"count1".>=(2)).select(KpiConstants.memberskColName)

    val measurementExcl2a = measurementDinominatorExcl2.intersect(measurementDinominator2aCountDf) /* Common Advanced Illness - measurementDinominatorExcl2 */

    /*Dinominator Exclusion3 Calculation (for primaryDiagnosisCodeSystem with valueset "Acute Inpatient"  )*/

    val joinForDinominatorExcl3a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpDinominatorExcl3aValueSet, KpiConstants.cbpDinominatorExcl3aCodeSystem)
    val measurementDinominatorExcl3a = UtilFunctions.mesurementYearFilter(joinForDinominatorExcl3a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName, KpiConstants.startDateColName).distinct()

    /*Atleast one Acute Inpatient*/
    val measurementDinominator3aCountDf = measurementDinominatorExcl3a.groupBy("member_sk").agg(count("start_date").alias("count1")).filter($"count1".>=(1)).select(KpiConstants.memberskColName)

    val measurementExcl3a = measurementDinominatorExcl2.intersect(measurementDinominator3aCountDf) /* Common Advanced Illness - measurementDinominatorExcl2 */

    /*Dinominator Exclusion4 Calculation (for primaryDiagnosisCodeSystem with valueset "Dementia Medications"  )*/
    val dimdateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)

    /*dispensed dementia medication (Dementia Medications List) starts*/
    val joinedForDemMedDf = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), dimMemberDf.col(KpiConstants.memberskColName) === factRxClaimsDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).join(ref_medvaluesetDf.as("df3"), factRxClaimsDf.col(KpiConstants.ndcNmberColName) === ref_medvaluesetDf.col(KpiConstants.ndcCodeColName), KpiConstants.innerJoinType).filter($"medication_list".isin(KpiConstants.spcDementiaMedicationListVal:_*)).select("df1.member_sk", "df2.start_date_sk")
    val startDateValAddedDfForDemMedDf = joinedForDemMedDf.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForDemMedDf = startDateValAddedDfForDemMedDf.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForDemMedDf = UtilFunctions.mesurementYearFilter(dateTypeDfForDemMedDf, "start_date", year, KpiConstants.measurementYearLower, KpiConstants.measuremetTwoYearUpper).select(KpiConstants.memberskColName)
    /*dispensed dementia medication (Dementia Medications List) ends*/

    val dinominatorExcl2BeforeAgeFilter = measurementExcl2a.union(measurementExcl3a).union(MeasurementForDemMedDf)

    val ageFilterDfForDinoExcl2 = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age66Val, KpiConstants.age80Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal).select(KpiConstants.memberskColName)

    val dinominatorExcl2 = dinominatorExcl2BeforeAgeFilter.intersect(ageFilterDfForDinoExcl2) /* Age between 66 - 80 */

    val dinominatorExclBeforeHospice = dinominatorExcl2.union(dinominatorExcl1)

    /*Dinominator Exclusion3 (Hospice)*/

    val hospiceDinoExclDf = UtilFunctions.hospiceMemberDfFunction(spark, dimMemberDf, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)

    val finalDinominatorExclDf = dinominatorExclBeforeHospice.union(hospiceDinoExclDf)/*dx*/

    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val finalDinominatorAfterExclDf = finalDinominatorBeforeExclDf.except(finalDinominatorExclDf).select(KpiConstants.memberskColName) /*d - dinominator - eligible population*/

    finalDinominatorAfterExclDf.show()

    /*Exclusions(optional)*/

    /*Optional Exclusion 1 */

    /*Exclusions for primaryDiagnosisCodeSystem with valueset "ESRD","Kidney Transplant" */

    val joinForOptionalExclusion1 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpOptionalExclusionICDValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val measurementOptionalExclusion1 = UtilFunctions.mesurementYearFilter(joinForOptionalExclusion1, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("df1.member_sk")

    /*Exclusions for proceedureCodeSystem with valueset "ESRD","ESRD Obsolete","Kidney Transplant" */

    val joinForOptionalExclusion1a = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpOptionalExclusion1ValueSet, KpiConstants.cbpOptionalExclusion1CodeSystem)
    val measurementOptionalExclusion1a = UtilFunctions.mesurementYearFilter(joinForOptionalExclusion1a, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("df1.member_sk")

    val optionalExclusion1 = measurementOptionalExclusion1.union(measurementOptionalExclusion1a)

    /*Optional Exclusion 2 */

    /*pregnancy Exclusion during measurement period*/

    val hedisJoinedForPregnancyExclusion = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,KpiConstants.cbpPregnancyExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDfPregnancyExclusion = UtilFunctions.mesurementYearFilter(hedisJoinedForPregnancyExclusion,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select("member_sk").distinct()

    /*Optional Exclusion 3 */

    /*Exclusions for proceedureCodeSystem with valueset "Inpatient Stay","Nonacute Inpatient Stay"*/

    val joinForOptionalExclusion3 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpOptionalExclusion2ValueSet, KpiConstants.cbpOptionalExclusion2CodeSystem)
    val measurementOptionalExclusion3 = UtilFunctions.mesurementYearFilter(joinForOptionalExclusion3, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper).select("df1.member_sk")


    val finalOptionalExclusionDf = 	optionalExclusion1.union(measurementDfPregnancyExclusion).union(measurementOptionalExclusion3).select(KpiConstants.memberskColName)

    finalOptionalExclusionDf.show()

    /* Final Dinominator */

    val dinominatorDf = finalDinominatorAfterExclDf.except(finalOptionalExclusionDf).select(KpiConstants.memberskColName) /* D */

    dinominatorDf.show()

    /*Numerator Calculation*/

    /* Numerator1 for "Outpatient Without UBREV","Nonacute Inpatient","Remote Blood Pressure Monitoring" valueset */


    val joinForNumerator1 = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumerator1ValueSet, KpiConstants.cbpNumerator1CodeSystem)
    val measurementNumerator1Df = UtilFunctions.mesurementYearFilter(joinForNumerator1, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val numerator1Df = ageFilterDf.as("df1").join(measurementNumerator1Df.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumerator1Df.col(KpiConstants.memberskColName), KpiConstants.innerJoinType)

    val numeratorSecondObservationDf = numerator1Df.withColumn("rank", row_number().over(Window.partitionBy($"member_sk").orderBy($"KpiConstants.startDateColName".asc))).filter($"rank"===(2))

    /*Numerator1 for "Systolic Less Than 140" valueset */

    val joinForNumeratorSystolic = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumeratorSystolicValueSet, KpiConstants.cbpNumeratorSystolicCodeSystem)
    val measurementNumeratorSystolicDf = UtilFunctions.mesurementYearFilter(joinForNumeratorSystolic, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val numeratorSystolicDf = ageFilterDf.as("df1").join(measurementNumeratorSystolicDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratorSystolicDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk","df2.start_date")

    /*Numerator1 for "Diastolic Less Than 80","Diastolic 80–89" valueset */

    val joinForNumeratorDiastolic = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dimMemberDf, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, KpiConstants.cbpNumeratorDiastolicValueSet, KpiConstants.cbpNumeratorDiastolicCodeSystem)
    val measurementNumeratorDiastolicDf = UtilFunctions.mesurementYearFilter(joinForNumeratorDiastolic, KpiConstants.startDateColName, year, KpiConstants.measurementYearLower, KpiConstants.measurementOneyearUpper)
    val numeratorDiastolicDf = ageFilterDf.as("df1").join(measurementNumeratorDiastolicDf.as("df2"), ageFilterDf.col(KpiConstants.memberskColName) === measurementNumeratorDiastolicDf.col(KpiConstants.memberskColName), KpiConstants.innerJoinType).select("df1.member_sk","df2.start_date")
    val numeratorSystolicDiastolicDf = numeratorSystolicDf.union(numeratorDiastolicDf)

    /* The BP reading must occur on or after the date of the second diagnosis of hypertension */

    val numeratorDf = numeratorSecondObservationDf.as("df1").join(numeratorSystolicDiastolicDf.as ("df2"), (numeratorSecondObservationDf.col(KpiConstants.memberskColName) === numeratorSystolicDiastolicDf.col(KpiConstants.memberskColName) &&  numeratorSecondObservationDf.col(KpiConstants.startDateColName) >= numeratorSystolicDiastolicDf.col(KpiConstants.startDateColName)), KpiConstants.innerJoinType).select("df1.member_sk")

    // val numeratorDf = numeratorSecondObservationDf.intersect(numeratorSystolicDiastolicDf)

    /*Final Numerator(Elements who are present in dinominator and numerator)*/

    val cbpFinalNumeratorDf = numeratorDf.intersect(dinominatorDf).select(KpiConstants.memberskColName).distinct()

    cbpFinalNumeratorDf.show()

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = KpiConstants.cbpNumerator1ValueSet ::: KpiConstants.cbpNumeratorSystolicValueSet ::: KpiConstants.cbpNumeratorDiastolicValueSet
    /*  val dinominatorExclValueSet = KpiConstants.cbpDinoExclValueSet ::: KpiConstants.cbpDinominatorExcl2aValueSet ::: KpiConstants.cbpDinominatorICDExcl2ValueSet ::: KpiConstants.cbpDinominatorExcl3aValueSet ::: KpiConstants.cbpDinominatorICDExcl2ValueSet */
    val dinominatorExclValueSet = KpiConstants.cbpOptionalExclusion1ValueSet ::: KpiConstants.cbpOptionalExclusionICDValueSet ::: KpiConstants.cbpPregnancyExclValueSet ::: KpiConstants.cbpOptionalExclusion2ValueSet
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.cbpMeasureId)
    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, finalOptionalExclusionDf, cbpFinalNumeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    //outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)



    /*Data populating to fact_hedis_qms*/
    //   val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.MeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    //   val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    //   val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    //  outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    spark.sparkContext.stop()
  }

}
