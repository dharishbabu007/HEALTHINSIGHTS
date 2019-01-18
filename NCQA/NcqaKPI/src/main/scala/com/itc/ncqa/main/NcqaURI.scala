package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConversions._


object NcqaURI {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAURI")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
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
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.uriMeasureTitle)


    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) ) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    //</editor-fold>

    //<editor-fold desc="Age Filter Calculation">

    val current_date = year + "-06-30"
    val last_Year_date = year.toInt -1 + "-07-01"
    val newDf1 = commonFilterDf.withColumn("curr_date", lit(current_date)).withColumn("last_Year_date", lit(last_Year_date))
    val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType)).withColumn("last_Year_date", newDf1.col("last_Year_date").cast(DateType))
    val ageFilterDf = newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).<=(KpiConstants.age18Val.toInt) && (datediff(newDf2.col("last_Year_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).>=(KpiConstants.age3Val.toInt))
                            .drop("curr_date","last_Year_date")


    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">
    /*Dinominator Calculation starts*/

    /*intake dates calculation*/
    val firstIntakeDate = year.toInt-1+"-07-01"
    val secondIntakeDate = year+"-06-30"


    //<editor-fold desc=" Step1">

    /* An observation visit (Observation Value Set) or an ED visit (ED Value Set)*/
    val uriObserEdValSet = List(KpiConstants.observationVal,KpiConstants.edVal)
    val uriObserEdCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForObserEdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.uriMeasureId,uriObserEdValSet,uriObserEdCodeSystem)
    val msrFilForObserEdDf = UtilFunctions.dateBetweenFilter(joinedForObserEdDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /* An Inpatient staty value set */

    val inPatValSet = List(KpiConstants.inpatientStayVal)
    val inPatCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinedForInPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.uriMeasureId,inPatValSet,inPatCodeSystem)
    val msrFilForInPatDf = UtilFunctions.dateBetweenFilter(joinedForInPatDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate)

    /* Ed and Observation without Inpatient Stay */
    val edObserWInPatDf = msrFilForObserEdDf.select(KpiConstants.memberskColName).except(msrFilForInPatDf.select(KpiConstants.memberskColName))

    /* Outpatient Value Set */

    val outPatValSet = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.uriMeasureId,outPatValSet,outPatCodeSystem)
    val msrFilForOutPatDf = UtilFunctions.dateBetweenFilter(joinedForOutPatDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName, KpiConstants.startDateColName)


    /*URI (URI Value Set) AS Primary Diagnosis*/

    val uriDiaValSet = List(KpiConstants.uriVal)
    val joinedForUriAsDiaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname ,KpiConstants.innerJoinType,KpiConstants.uriMeasureId,uriDiaValSet,primaryDiagnosisCodeSystem)
    val msrFilUriisAsDiagDf = UtilFunctions.dateBetweenFilter(joinedForUriAsDiaDf ,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate)

    /* Outpatient Value Set, Observation Value Set,ED visit with only diagnoses of URI */
    val step1Df = (msrFilForOutPatDf.select(KpiConstants.memberskColName).union(edObserWInPatDf)).intersect(msrFilUriisAsDiagDf.select(KpiConstants.memberskColName))
    //</editor-fold>

    //<editor-fold desc=" Step2">

    /* Identify Episode Date - Ed and Outpatient Valueset during Intake period with URI Value Set */
    val edobsoutDf = msrFilForObserEdDf.union(msrFilForOutPatDf)
    val step2Df = edobsoutDf.as("df1").join(step1Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                   .select(s"df2.${KpiConstants.memberskColName}", s"df1.${KpiConstants.startDateColName}")
    //</editor-fold>

    //<editor-fold desc="Step3 ">

    /* Determine if antibiotics (URI Antibiotic Medications List) were dispensed for any of the Episode Dates */


    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    val cwpAntiMedListVal = List(KpiConstants.cwpAntibioticMedicationListsVal)
    val cwpAntiListDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factClaimDf,ref_medvaluesetDf,KpiConstants.uriMeasureId,cwpAntiMedListVal)
                                     .select(KpiConstants.memberskColName, KpiConstants.rxStartDateColName)

    /* CWP medication was filled 30 days prior to the Episode Date or was active on the Episode Date */

    val step3ConditionDf = step2Df.as("df1").join(cwpAntiListDf.as("df2"), step2Df.col(KpiConstants.memberskColName) === cwpAntiListDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                          .filter(datediff(step2Df.col(KpiConstants.startDateColName),cwpAntiListDf.col(KpiConstants.rxStartDateColName)).<=(KpiConstants.days30))
                                          .select(step2Df.col(KpiConstants.memberskColName),step2Df.col(KpiConstants.startDateColName))

    val step3Df = step2Df.except(step3ConditionDf)
    //</editor-fold>

    //<editor-fold desc="Step4 ">

    /* 	Pharyngitis Value Set, Competing Diagnosis Value Set  */

    val pharComDiaValList = List(KpiConstants.pharyngitisVal,KpiConstants.competingDiagnosisVal)
    val joinForpharComDiaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname ,KpiConstants.innerJoinType,KpiConstants.uriMeasureId,pharComDiaValList,primaryDiagnosisCodeSystem)
                                           .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /* Competing diagnosis on or three days after the Episode Date */
    val step4ConditionDf = step3Df.as("df1").join(joinForpharComDiaDf.as("df2"), step3Df.col(KpiConstants.memberskColName) === joinForpharComDiaDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                          .filter(datediff(joinForpharComDiaDf.col(KpiConstants.startDateColName),step3Df.col(KpiConstants.startDateColName)).>=(KpiConstants.days3))
                                          .select(step3Df.col(KpiConstants.memberskColName),step3Df.col(KpiConstants.startDateColName))

    val step4Df = step3Df.except(step4ConditionDf)
    //</editor-fold>

    //<editor-fold desc="Step5">

    /* Continuous enrollment for 34 days */
    val contEnrollDateDf = step4Df.withColumn(KpiConstants.contenrollLowCoName, date_sub(step4Df.col(KpiConstants.startDateColName), KpiConstants.days30))
                                  .withColumn(KpiConstants.contenrollUppCoName, date_add(step4Df.col(KpiConstants.startDateColName), KpiConstants.days3))


    val step5Df = contEnrollDateDf.as("df1").join(ageFilterDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                             .filter(($"df2.${KpiConstants.memStartDateColName}".<=($"df1.${KpiConstants.contenrollLowCoName}")) && ($"df2.${KpiConstants.memEndDateColName}".>=($"df1.${KpiConstants.contenrollUppCoName}")))
                                             .select(s"df1.${KpiConstants.memberskColName}", s"df1.${KpiConstants.startDateColName}")
    //</editor-fold>

    //<editor-fold desc="Step6">

    /* IESD - This measure examines the earliest eligible episode per member */

    val step6Df = step5Df.groupBy(KpiConstants.memberskColName).agg(min(KpiConstants.startDateColName).alias(KpiConstants.iesdDateColName))
    //</editor-fold>



    val dinominatorDf = ageFilterDf.as("df1").join(step6Df.as("df2"),$"df1.${KpiConstants.memEndDateColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                   .select("df1.*")

    val dinominatroForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val dinominatorExclDf = hospiceDinoExclDf
    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val dinominatorAfterExclDf = dinominatroForKpiCalDf.except(dinominatorExclDf)

    dinominatorAfterExclDf.show()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /* CWP Antibiotic Medications List on or three days after the IESD */

    val numCwpDf = cwpAntiListDf.as("df1").join(step6Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                 .filter((datediff($"df1.${KpiConstants.rxStartDateColName}", $"df2.${KpiConstants.iesdDateColName}").===(KpiConstants.days3)) &&
                                                   (datediff($"df1.${KpiConstants.rxStartDateColName}", $"df2.${KpiConstants.iesdDateColName}").===(KpiConstants.days0)))
                                                  .select(s"df1.${KpiConstants.memberskColName}")

    val numeratorDf = numCwpDf.intersect(dinominatorAfterExclDf)

    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = cwpAntiMedListVal
    val dinominatorExclValueSet = KpiConstants.emptyList
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
