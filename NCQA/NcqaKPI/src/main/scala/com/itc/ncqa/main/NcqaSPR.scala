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

object NcqaSPR {
/*

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
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
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
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.sprMeasureTitle)

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) || (KpiConstants.medicareLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    //</editor-fold>

    //<editor-fold desc="Age Filter Calculation">

    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf, KpiConstants.dobColName, year, KpiConstants.age40Val, KpiConstants.age120Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*intake dates calculation*/
    val firstIntakeDate = year.toInt - 1 + "-07-01"
    val secondIntakeDate = year + "-06-30"

    //<editor-fold desc="Step1">

    //<editor-fold desc="Step1Sub1(outpatient, ED,Observation with COPD,Emphysema,chronic bronchits)">

    /* An observation visit (Observation Value Set) or an ED visit (ED Value Set)*/
    val sprObserEdValSet = List(KpiConstants.observationVal, KpiConstants.edVal)
    val sprObserEdCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForObserEdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.sprMeasureId, sprObserEdValSet, sprObserEdCodeSystem)
    val msrFilForObserEdDf = UtilFunctions.dateBetweenFilter(joinedForObserEdDf, KpiConstants.startDateColName, firstIntakeDate, secondIntakeDate)
                                          .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /* An Inpatient stay value set */
    val inPatValSet = List(KpiConstants.inpatientStayVal)
    val inPatCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinedForInPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.sprMeasureId, inPatValSet, inPatCodeSystem)
    val msrFilForInPatDf = UtilFunctions.dateBetweenFilter(joinedForInPatDf, KpiConstants.startDateColName, firstIntakeDate, secondIntakeDate)

    /* Ed and Observation without Inpatient Stay */
    val edObserWOInPatDf = msrFilForObserEdDf.select(KpiConstants.memberskColName).except(msrFilForInPatDf.select(KpiConstants.memberskColName))

    /* Outpatient Value Set */

    val outPatValSet = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.sprMeasureId, outPatValSet, outPatCodeSystem)
    val msrFilForOutPatDf = UtilFunctions.dateBetweenFilter(joinedForOutPatDf, KpiConstants.startDateColName, firstIntakeDate, secondIntakeDate)
                                         .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*SPR (COPD, EMPHYSEMA, Chronic Bronchitis Value Set) AS Primary Diagnosis*/
    val sprDiaValueSet = List(KpiConstants.copdVal, KpiConstants.emphysemaVal, KpiConstants.chronicBronchitisVal)
    val hedisJoinedSprAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.sprMeasureId, sprDiaValueSet, KpiConstants.primaryDiagnosisCodeSystem)
    val msrFilSprisAsDiagDf = UtilFunctions.dateBetweenFilter(hedisJoinedSprAsDiagDf, KpiConstants.startDateColName, firstIntakeDate, secondIntakeDate)
                                           .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*outpatient and copd*/
    val outPatAndcopdDf = msrFilForOutPatDf.select(KpiConstants.memberskColName).intersect(msrFilSprisAsDiagDf.select(KpiConstants.memberskColName))
    /*ed,obs with copd*/
    val edobsAndcopdDf = edObserWOInPatDf.intersect(msrFilSprisAsDiagDf.select(KpiConstants.memberskColName))

    /* Outpatient Value Set, Observation Value Set,ED visit with only diagnoses for COPD */
    val outPatObEdWCopdDf = outPatAndcopdDf.union(edobsAndcopdDf)

    /* Do not include telehealth (Telehealth Modifier Value Set; Telehealth POS Value Set) */
    val sprTelehealthVal = List(KpiConstants.telehealthModifierVal, KpiConstants.telehealthPosVal)
    val sprTelehealthCodeSystem = List(KpiConstants.modifierCodeVal, KpiConstants.posCodeVal)
    val joinedForSprTelehealthDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.sprMeasureId, sprTelehealthVal, sprTelehealthCodeSystem)
    val msrFilterForSprTelehealthDf = UtilFunctions.dateBetweenFilter(joinedForSprTelehealthDf, KpiConstants.startDateColName, firstIntakeDate, secondIntakeDate).select(KpiConstants.memberskColName, KpiConstants.startDateColName)
    /*Step1Sub1(outpatient, ED,Observation with COPD,Emphysema,chronic bronchits)*/
    val step1Sub1Df = outPatObEdWCopdDf.select(KpiConstants.memberskColName).except(msrFilterForSprTelehealthDf).select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Step1Sub2(An acute inpatient discharge with any diagnosis of COPD  EMPHYSEMA, Chronic Bronchitis Value Set)">

    /* Identify all acute and nonacute inpatient stays (Inpatient Stay Value Set) */
    val mesrForInPatDisDf = UtilFunctions.dateBetweenFilter(joinedForInPatDf, KpiConstants.dischargeDateColName, firstIntakeDate, secondIntakeDate)
                                         .select(KpiConstants.memberskColName, KpiConstants.dischargeDateColName)

    /* Exclude nonacute inpatient stays (Nonacute Inpatient Stay Value Set) */
    val sprNonAcuteVal = List(KpiConstants.nonAcuteInPatientStayVal)
    val sprNonAcuteCodeSystem = List(KpiConstants.ubrevCodeVal,KpiConstants.ubtobCodeVal)
    val joinedForSprNonAcuteDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.sprMeasureId,sprNonAcuteVal,sprNonAcuteCodeSystem)
    val msrFilterForSprNonAcuteDf = UtilFunctions.dateBetweenFilter(joinedForSprNonAcuteDf,KpiConstants.dischargeDateColName,firstIntakeDate,secondIntakeDate)
                                                 .select(KpiConstants.memberskColName, KpiConstants.dischargeDateColName)

    val acuteInPatDf = mesrForInPatDisDf.select(KpiConstants.memberskColName).except(msrFilterForSprNonAcuteDf.select(KpiConstants.memberskColName))
    val acuteInpatAndFilSprisDf = acuteInPatDf.intersect(msrFilSprisAsDiagDf.select(KpiConstants.memberskColName))
    /*acute Inpatient discharge date in intake period and has COPD or emphysema or chronic bronchitis*/
    val acuteInpatDisDateDf = mesrForInPatDisDf.as("df1").join(acuteInpatAndFilSprisDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                                .select("df1.*")

    val step1Sub2Df = acuteInpatDisDateDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    val step1Df = step1Sub1Df.union(step1Sub2Df)
    //</editor-fold>

    //<editor-fold desc=" Step2">

    //<editor-fold desc="Step2Sub1">

    /* Telehealth Modifier Value Set, Telephone Visits Value Set, Online Assessments Value Set)*/
    val teletOnlineValueSet = List(KpiConstants.telehealthModifierVal,KpiConstants.telephoneVisitsVal,KpiConstants.onlineAssesmentVal)
    val teleOnlineCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.modifierCodeVal)
    val joinedForteletOnlineDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType ,KpiConstants.sprMeasureId,teletOnlineValueSet,teleOnlineCodeSystem)
                                              .select(KpiConstants.memberskColName)
    /*outpatient with or without telehealth,telephone,Online visit*/
    val outPatTeleOnlineHisDf = joinedForOutPatDf.select(KpiConstants.memberskColName).intersect(joinedForteletOnlineDf)

    /*out patient and copd history*/
    val outTeleOnlineAndcopdDf = outPatTeleOnlineHisDf.intersect(hedisJoinedSprAsDiagDf.select(KpiConstants.memberskColName))

    val outPatstartDateDf = joinedForOutPatDf.as("df1").join(outTeleOnlineAndcopdDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                             .select(s"df1.${KpiConstants.memberskColName}",s"df1.${KpiConstants.startDateColName}")
    /*outpatient with IESD date*/
    val outPatiesdDateDf = outPatstartDateDf.withColumn(KpiConstants.iesdDateColName, date_sub($"${KpiConstants.startDateColName}", KpiConstants.days730))

    /*ed obs history data without Inpat*/
    val edobsHisDf = joinedForObserEdDf.select(KpiConstants.memberskColName).except(joinedForInPatDf.select(KpiConstants.memberskColName))

    /*ED,Observation with copd history*/
    val edobsCobdDf = edobsHisDf.intersect(hedisJoinedSprAsDiagDf.select(KpiConstants.memberskColName))

    val edobsstartDateDf = joinedForObserEdDf.as("df1").join(edobsCobdDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                             .select(s"df1.${KpiConstants.memberskColName}",s"df2.${KpiConstants.startDateColName}")
    /*ED,Observation with iesd date added*/
    val edobsiesdDateDf = edobsstartDateDf.withColumn(KpiConstants.iesdDateColName, date_sub($"${KpiConstants.startDateColName}", KpiConstants.days730))

    val edobsOutiesdDf = outPatiesdDateDf.union(edobsiesdDateDf)

    /*Outpatient, ED, Observation who has some service during the 730 days prior to the iesd*/
    val step2Sub1Df = edobsOutiesdDf.as("df1").join(edobsOutiesdDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                    .filter(($"df2.${KpiConstants.startDateColName}".>=($"df1.${KpiConstants.iesdDateColName}")) && ($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.startDateColName}")))
                                    .select(s"df1.${KpiConstants.memberskColName}")

    //</editor-fold>

    //<editor-fold desc="Step2Sub2">

    /*acute Inpatient History Data*/
   val acuteInPatHisDf = joinedForInPatDf.select(KpiConstants.memberskColName).except(joinedForSprNonAcuteDf.select(KpiConstants.memberskColName))

   /*AcuteInpatient with copd*/
   val acuteInpatAndCpdDf =  acuteInPatHisDf.intersect(hedisJoinedSprAsDiagDf.select(KpiConstants.memberskColName))

   val acuteInPatStDisDateDf =  joinedForInPatDf.as("df1").join(acuteInpatAndCpdDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}")
                                              .select(s"df1.${KpiConstants.memberskColName}", s"df1.${KpiConstants.admitDateColName}", s"df1.${KpiConstants.dischargeDateColName}")

   val acuteInPatiesdDateDf =  acuteInPatStDisDateDf.withColumn(KpiConstants.iesdDateColName, date_sub($"${KpiConstants.admitDateColName}", KpiConstants.days730))

   val step2Sub2Df = acuteInPatiesdDateDf.as("df1").join(acuteInPatiesdDateDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                         .filter(($"df2.${KpiConstants.dischargeDateColName}".>=($"df1.${KpiConstants.iesdDateColName}")) && ($"df2.${KpiConstants.dischargeDateColName}".<=($"df1.${KpiConstants.admitDateColName}")))
                                         .select(s"df1.${KpiConstants.memberskColName}")
    //</editor-fold>

    val step2Df = step2Sub1Df.union(step1Sub2Df)
    //</editor-fold>

    //<editor-fold desc="Step3">

    /*Continuous enrollment Lower and upper date added to copd Df */
    val contEnrollDateDf = msrFilSprisAsDiagDf.withColumn(KpiConstants.contenrollLowCoName, date_sub(msrFilSprisAsDiagDf.col(KpiConstants.startDateColName), KpiConstants.days730))
                                              .withColumn(KpiConstants.contenrollUppCoName, date_add(msrFilSprisAsDiagDf.col(KpiConstants.startDateColName), KpiConstants.days180))

    val step3Df = contEnrollDateDf.as("df1").join(ageFilterDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                  .filter(($"df2.${KpiConstants.memStartDateColName}".<=($"df1.${KpiConstants.contenrollLowCoName}"))
                                    && ($"df2.${KpiConstants.memEndDateColName}".>=($"df1.${KpiConstants.contenrollUppCoName}")))
                                  .select(s"df1.${KpiConstants.memberskColName}")
    //</editor-fold>


    val dinominatorUnionDf = (step1Df.intersect(step3Df)).except(step2Df)
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorUnionDf.as("df2"),$"df1.${KpiConstants.memEndDateColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                    .select("df1.*")

    val dinominatroForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclusion (Hospice)*/
    val hospiceDinoExclDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName)
    val dinominatorExclDf = hospiceDinoExclDf
    /*Final Dinominator (Dinominator - Dinominator Exclusion)*/
    val dinominatorAfterExclDf = dinominatroForKpiCalDf.except(dinominatorExclDf)
   //dinominatorAfterExclDf.show()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /* Spirometry value Set */

    val sprNumVal = List(KpiConstants.spirometryVal)
    val sprNumCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForSprNumDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.sprMeasureId,sprNumVal,sprNumCodeSystem)


    val numSprDf = joinedForSprNumDf.as("df1").join(contEnrollDateDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
      .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.contenrollLowCoName}")) &&
        ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.contenrollUppCoName}")))
      .select(s"df1.${KpiConstants.memberskColName}")

    val numeratorDf = numSprDf.intersect(dinominatorAfterExclDf)

    //numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = sprNumVal
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
*/
}
