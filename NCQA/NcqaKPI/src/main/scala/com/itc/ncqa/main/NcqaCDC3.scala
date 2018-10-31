package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.UtilFunctions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object NcqaCDC3 {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACDC2")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val year = args(0)
    val lob_id = args(1)
    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership tables*/
    val dimMemberDf_init = spark.sql(KpiConstants.dimMemberLoadQuery)
    val dimMemberDfColumns = dimMemberDf_init.columns.map(f => f.toUpperCase)
    val dimMemberDf = UtilFunctions.removeHeaderFromDf(dimMemberDf_init, dimMemberDfColumns, "member_sk")
    val factClaimDf_init = spark.sql(KpiConstants.factClaimLoadQuery)
    val factClaimDfColumns = factClaimDf_init.columns.map(f => f.toUpperCase)
    val factClaimDf = UtilFunctions.removeHeaderFromDf(factClaimDf_init, factClaimDfColumns, "member_sk")
    val factMembershipDf_init = spark.sql(KpiConstants.factMembershipLoadQuery)
    val factMembershipDfColumns = factMembershipDf_init.columns.map(f => f.toUpperCase())
    val factMembershipDf = UtilFunctions.removeHeaderFromDf(factMembershipDf_init, factMembershipDfColumns, "member_sk")
    val factRxClaimsDf_init = spark.sql(KpiConstants.factRxClaimLoadQuery)
    val factRxClaimsColumns = factRxClaimsDf_init.columns
    val factRxClaimsDf = UtilFunctions.removeHeaderFromDf(factRxClaimsDf_init,factRxClaimsColumns,"member_sk")
    val ref_lobDf = spark.sql(KpiConstants.refLobLoadQuery)


    val joinedDimMemberAndFctclaimDf = dimMemberDf.as("df1").join(factClaimDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.member_sk", KpiConstants.arrayOfColumn: _*)
    val joinedFactMembershipDf = joinedDimMemberAndFctclaimDf.as("df1").join(factMembershipDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").select("df1.*", "df2.product_plan_sk", "df2.lob_id")
    val joinedWithLobDf = joinedFactMembershipDf.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.*","df2.lob_name")

    /*load the look up view */
    val lookUpTableDf = spark.sql(KpiConstants.view45DaysLoadQuery)
    //lookUpTableDf.printSchema()



    /*Removing the elements who has a gap of 45 days*/
    val commonFilterDf = joinedFactMembershipDf.as("df1").join(lookUpTableDf.as("df2"), $"df1.member_sk" === $"df2.member_sk", "left_outer").filter("start_date is null").select("df1.*")

    /*converting dateofbirthsk to dob in date format*/
    val dimdateDf = spark.sql(KpiConstants.dimDateLoadQuery)
    val dobDateValAddedDf = commonFilterDf.as("df1").join(dimdateDf.as("df2"), $"df1.DATE_OF_BIRTH_SK" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "dob_temp").drop("DATE_OF_BIRTH_SK")
    val dateTypeDf = dobDateValAddedDf.withColumn("dob", to_date($"dob_temp", "dd-MMM-yyyy")).drop("dob_temp")

    /*doing age filter */
    val ageFilterDf = UtilFunctions.ageFilter(dateTypeDf, "dob", year, "18", "75",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal) .select("member_sk","dob").distinct()
    /*loading ref_hedis table*/
    val refHedisDf = spark.sql(KpiConstants.refHedisLoadQuery)

    /*calculating Dinominator*/

    /*Dinominator First condition */
    val hedisJoinedForFirstDino = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc4DiabetesvalueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementForFirstDino = UtilFunctions.mesurementYearFilter(hedisJoinedForFirstDino,"start_date",year,0,730).select("member_sk","start_date")
    val firstDinominatorDf = measurementForFirstDino.select("member_sk")

    /*Dinominator Second Condition*/
    val ref_medvaluesetDf = spark.sql(KpiConstants.refmedvaluesetLoadQuery)
    val medValuesetForThirdDino = dimMemberDf.as("df1").join(factRxClaimsDf.as("df2"), $"df1.member_sk" === $"df2.member_sk").join(ref_medvaluesetDf.as("df3"),$"df2.ndc_number" === $"df3.ndc_code","inner").filter($"measure_id".===("CHL")).select("df1.member_sk","df2.start_date_sk")
    val startDateValAddedDfForSeconddDino = medValuesetForThirdDino.as("df1").join(dimdateDf.as("df2"), $"df1.start_date_sk" === $"df2.date_sk").select($"df1.*", $"df2.calendar_date").withColumnRenamed("calendar_date", "start_temp").drop("start_date_sk")
    val dateTypeDfForSecondDino = startDateValAddedDfForSeconddDino.withColumn("start_date", to_date($"start_temp", "dd-MMM-yyyy")).drop("start_temp")
    val MeasurementForSecondDinoDf = UtilFunctions.mesurementYearFilter(dateTypeDfForSecondDino,"start_date",year,0,730).select("member_sk","start_date")
    val secondDinominatorDf = MeasurementForSecondDinoDf.select("member_sk")


    /*union of first and second dinominator condition*/
    val dinominatorUnionDf = firstDinominatorDf.union(secondDinominatorDf).distinct()
    /*dinominator Calculation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk")
    //dinominatorDf.show(50)





    /*dinominator Exclusion 1*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    val measurementDinominatorExclDf = UtilFunctions.mesurementYearFilter(hospiceDf,"start_date",year,0,365).select("member_sk").distinct()

    /*dinominator Exclusion 2*/
    val hedisJoinedForDiabetesExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdcDiabetesExclValueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementDiabetesExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDiabetesExclDf,"start_date",year,0,730).select("member_sk").distinct()


    /*dinominator Exclusion 3 (Age filter  Exclusion)*/
    val dinominatorAgeFilterExclDf = UtilFunctions.ageFilter(ageFilterDf,"dob",year,"65","75",KpiConstants.boolTrueVal,KpiConstants.boolTrueVal).select("member_sk").distinct()

    /*dinominator Exclusion 4 (CABG Exclusion)*/
    val hedisJoinedForCabgValueExclDf =  UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3CabgValueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementCabgValueExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForCabgValueExclDf,"start_date",year,0,730).select("member_sk").distinct()

    /*dinominator Exclusion 5 (PCI exclusion)*/
    val hedisJoinedForPciValueExclDf =  UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3PciValueSet,KpiConstants.cdc4DiabetescodeSystem)
    val measurementPciValueExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForPciValueExclDf,"start_date",year,0,730).select("member_sk").distinct()




    /*dinominator Exclusion 6 (IVD condition)*/
    val hedisJoinedForOutPatExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3OutPatientValueSet,KpiConstants.cdc3OutPatientCodeSystem)
    val measurementOutPatExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForOutPatExclDf,"start_date",year,0,730)
    /*IVD EXCL*/
    val hedisJoinedForIvdExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3IvdExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementIvdExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForIvdExclDf,"start_date",year,0,730)

    /*join measurementOutPatExclDf and measurementIvdExclDf for getting the member_sk that are present in both the case*/
    val joinOutPatAndIvdDf = measurementOutPatExclDf.as("df1").join(measurementIvdExclDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk").distinct()


    //Acute Inpatient
    val hedisJoinedForAccuteInPatExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3AccuteInPtValueSet,KpiConstants.cdc3AccuteInPtCodeSystem)
    val measurementAccuteInPatExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForAccuteInPatExclDf,"start_date",year,0,730)

    /*Join Acute Inpatient with Ivd Excl*/
    val joinAccuteInPatAndIvdDf = measurementAccuteInPatExclDf.as("df1").join(measurementIvdExclDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk").distinct()

    /*union of joinOutPatAndIvdDf and joinAccuteInPatAndIvdDf*/
    val ivdExclDf = joinOutPatAndIvdDf.union(joinAccuteInPatAndIvdDf)

    /*dinominator Exclusion 6 (IVD condition) Ends */






    /*dinominator Exclusion 7 (Thoracic aortic aneurysm)*/
    val hedisJoinedForThAoAnExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3ThAoAnvalueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementThAoAnExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForThAoAnExclDf,"start_date",year,0,730)

    /*join measurementOutPatExclDf with measurementThAoAnExclDf for the first condition*/
    val joinedOutPatAndThAoAnExclDf = measurementOutPatExclDf.as("df1").join(measurementThAoAnExclDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk").distinct()

    /*join measurementAccuteInPatExclDf with measurementThAoAnExclDf for the second condition*/
    val joinedAccuteInAndThAoAnExclDf = measurementAccuteInPatExclDf.as("df1").join(measurementThAoAnExclDf.as("df2"),$"df1.member_sk" === $"df2.member_sk","inner").select("df1.member_sk").distinct()

    /*union of joinedOutPatAndThAoAnExclDf and joinedAccuteInAndThAoAnExclDf*/
    val thAoAnExclDf = joinedOutPatAndThAoAnExclDf.union(joinedAccuteInAndThAoAnExclDf)

    /*dinominator Exclusion 7 (Thoracic aortic aneurysm) ends*/





    /*Dinominator Exclusion 8 Starts*/

    /*Chronic Heart Failure Exclusion*/
    val hedisJoinedForChfExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3ChfExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementChfExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForChfExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*MI*/
    val hedisJoinedForMiExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3MiExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementMiExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMiExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*ESRD AS Primary Diagnosis */
    val hedisJoinedForEsrdAsDiagExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3MiExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementEsrdAsDiagExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForMiExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*ESRD and  ESRD Obsolete as proceedure code*/
    val hedisJoinedForEsrdAsProExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3EsrdExclValueSet,KpiConstants.cdc3EsrdExclcodeSystem)
    val measurementEsrdAsProExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForEsrdAsProExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*ESRD Exclusion by union ESRD AS Primary Diagnosis and ESRD and  ESRD Obsolete as proceedure code*/
    val esrdExclDf = measurementEsrdAsDiagExclDf.union(measurementEsrdAsProExclDf)

    /*CKD Stage 4  Exclusion*/
    val hedisJoinedForckdExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3CkdStage4ValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementckdExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForckdExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*Dementia Exclusion*/
    val hedisJoinedForDementiaExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3DementiaExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementDementiaExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForDementiaExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*Blindness Exclusion*/
    val hedisJoinedForBlindnessExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3BlindnessExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementBlindnessExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForBlindnessExclDf,"start_date",year,0,365).select("member_sk").distinct()


    /* Lower extremity amputation Exclusion as primary diagnosis*/
    val hedisJoinedForLeaAsDiagExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"primary_diagnosis","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3LEAExclValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val measurementLeaAsDiagExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLeaAsDiagExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*Lower extremity amputation Exclusion as Proceedure Code*/
    val hedisJoinedForLeaAsProExclDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc3LEAExclValueSet,KpiConstants.cdc3LEAExclCodeSystem)
    val measurementLeaAsProExclDf = UtilFunctions.mesurementYearFilter(hedisJoinedForLeaAsProExclDf,"start_date",year,0,365).select("member_sk").distinct()

    /*union of measurementLeaAsDiagExclDf and measurementLeaAsProExclDf for Lower extremity amputation condition*/
    val leaExclDf = measurementLeaAsDiagExclDf.union(measurementLeaAsProExclDf).distinct()





    /*union of all Dinominator8 Exclusions*/
    val unionOfAllDinominator8ExclDf = measurementChfExclDf.union(measurementMiExclDf).union(esrdExclDf).union(measurementDementiaExclDf).union(measurementBlindnessExclDf).union(leaExclDf).union(measurementckdExclDf)

    /*Union of all 8 Dinominator Exclusion Condition*/
    val unionOfAllDinominatorExclDf = measurementDinominatorExclDf.union(measurementDiabetesExclDf).union(dinominatorAgeFilterExclDf).union(measurementCabgValueExclDf).union(measurementPciValueExclDf).union(ivdExclDf).union(thAoAnExclDf).union(unionOfAllDinominator8ExclDf).distinct()
    //unionOfAllDinominatorExclDf.show(50)


    /*CDC3 Dinominator for kpi calculation */
    val cdc3DinominatorForKpiDf = dinominatorDf.except(unionOfAllDinominatorExclDf)

    /*Numerator Calculation*/


    /*Numerator 1 (who has done Hemoglobin A1c (HbA1c) testing)*/
    val hedisJoinedForHba1cDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc1NumeratorValueSet,KpiConstants.cdc1NumeratorCodeSystem)
    val measurementForHba1cDf = UtilFunctions.mesurementYearFilter(hedisJoinedForHba1cDf,"start_date",year,0,365).select("member_sk","start_date").select("member_sk").distinct()

    /*Numerator2 (HbA1c Level Greater Than 9.0)*/
    val hedisJoinedForNumeratorDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.cdcMeasureId,KpiConstants.cdc2NumeratorValueSet,KpiConstants.cdc2NumeratorCodeSystem)
    val measurementForNumerator = UtilFunctions.mesurementYearFilter(hedisJoinedForNumeratorDf,"start_date",year,0,365).select("member_sk")
    val numeratorDf = measurementForNumerator.intersect(measurementForHba1cDf)

    val cdc3NumeratorDf = numeratorDf.intersect(cdc3DinominatorForKpiDf)

    /*outputformat*/
    val lobdetailsData = dinominatorDf.as("df1").join(factMembershipDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df1.member_sk","df2.lob_id")
    val payerNamedAdded = lobdetailsData.as("df1").join(ref_lobDf.as("df2"),$"df1.lob_id" === $"df2.lob_id").select("df1.member_sk","df2.lob_name")
    val dataDf = payerNamedAdded.as("df1").join(dimMemberDf.as("df2"),$"df1.member_sk" === $"df2.member_sk").select("df2.member_id","df1.lob_name")

    val formattedOutPutDf = UtilFunctions.outputDfCreation(spark,dataDf,unionOfAllDinominatorExclDf,cdc3NumeratorDf,dimMemberDf,KpiConstants.cdcMeasureId)
    formattedOutPutDf.orderBy("MemID").show(100)
  }

}
