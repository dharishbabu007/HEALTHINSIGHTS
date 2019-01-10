package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


object NcqaOMW {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and Spark session object creation">

    /*Reading the program arguments*/
    val year = args(0)
    val lob_name = args(1)
    val programType = args(2)
    val dbName = args(3)
    var data_source = ""
    val measureId = args(4)

    /*define data_source based on program type. */
    if ("ncqatest".equals(programType)) {
      data_source = KpiConstants.ncqaDataSource
    }
    else {
      data_source = KpiConstants.clientDataSource
    }

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAOMW")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required tables to Memory">

    import spark.implicits._


    /*Loading dim_member,fact_claims,fact_membership , dimLocationDf, refLobDf, dimFacilityDf, factRxClaimsDf tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName,data_source)
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refmedValueSetTblName)
    //</editor-fold>

    //<editor-fold desc="Initial join, Allowable Gap Filter ,Age filter and Gender filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.omwMeasureTitle)

    val lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")
    /*Age & Gender filter*/
    val ageFilterDf = UtilFunctions.ageFilter(commonFilterDf,KpiConstants.dobColName,year,KpiConstants.age67Val,KpiConstants.age85Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    val genderFilterDf = ageFilterDf.filter($"gender".===("F"))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)

    /*intake dates calculation*/
    val firstIntakeDate = year.toInt-1+"-07-01"
    val secondIntakeDate = year+"-06-30"

    //<editor-fold desc="Step1">

    //<editor-fold desc="outpatient visit (Outpatient Value Set)">

    val omwOutPatientValueSet = List(KpiConstants.outPatientVal)
    val omwOutPatientCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForOutpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwOutPatientValueSet,omwOutPatientCodeSystem)
    val mesurForOutpatDf = UtilFunctions.dateBetweenFilter(joinedForOutpatDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    //</editor-fold>

    //<editor-fold desc="Fracture without Telehealth">

    /*fracture (Fractures Value Set) AS Primary Diagnosis*/
    val omwFractureValueSet = List(KpiConstants.fracturesVal)
    val joinedForFractureAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwFractureValueSet,primaryDiagCodeSystem)
    val mesurForFractureAsDiagDf = UtilFunctions.dateBetweenFilter(joinedForFractureAsDiagDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*fracture (Fractures Value Set) AS Procedure Code*/
    val omwFractureCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
    val hedisJoinedForFractureAsProDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwFractureValueSet,omwFractureCodeSystem)
    val mesurForFractureAsProDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsProDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Fracture Dinominator (Union of Fractures Value Set AS Primary Diagnosis and Fractures Value Set AS Procedure Code)*/
    val fractureUnionDf = mesurForFractureAsDiagDf.union(mesurForFractureAsProDf)

    val telehealthmodValList = List(KpiConstants.telehealthModifierVal, KpiConstants.telehealthPosVal)
    val telehealthmodCodeSystem = List(KpiConstants.posCodeVal, KpiConstants.modifierCodeVal)
    val joinedForTeleheamodDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,telehealthmodValList,telehealthmodCodeSystem)
    val mesurForTeleheamodDf = UtilFunctions.dateBetweenFilter(joinedForTeleheamodDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName)

    val fracturewotelehealthDf = fractureUnionDf.select(KpiConstants.memberskColName).except(mesurForTeleheamodDf)
    //</editor-fold>

    //<editor-fold desc="ED, Observation without Inpatient stay">

    /*Inpatient stay*/
    val omwInpatientStayValueSet = List(KpiConstants.inpatientStayVal)
    val omwInpatientStayCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinedForInpatientStDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwInpatientStayValueSet,omwInpatientStayCodeSystem)
    val mesurForInPatientStayDf = UtilFunctions.dateBetweenFilter(joinedForInpatientStDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate)

    /*ED or Observation visit */
    val observEdValueSet = List(KpiConstants.observationVal,KpiConstants.edVal)
    val observEdCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForObservEdDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,observEdValueSet,observEdCodeSystem)
    val mesurForObservEdDf = UtilFunctions.dateBetweenFilter(joinedForObservEdDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val edobsWoInpatDf = mesurForObservEdDf.select(KpiConstants.memberskColName).except(mesurForInPatientStayDf)
    //</editor-fold>

    val step1_sub1Df = (mesurForOutpatDf.select(KpiConstants.memberskColName).union(edobsWoInpatDf)).intersect(fracturewotelehealthDf)

    /*Mmebers who has fracture and Acute and Non acute*/
    val mesrForInpatDisDf = UtilFunctions.dateBetweenFilter(joinedForObservEdDf,KpiConstants.dischargeDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName)
    val fractureAndInpatDf = fractureUnionDf.intersect(mesrForInpatDisDf.select(KpiConstants.memberskColName))

    val step1Df = step1_sub1Df.union(fractureAndInpatDf)
    //</editor-fold>

    //<editor-fold desc="Step2">

    //<editor-fold desc="Step2 Sub1">

    /* Telephone Visits Value Set, Online Assessments Value Set)*/
    val teleOnlinevisitValueList = List(KpiConstants.telephoneVisitsVal,KpiConstants.onlineAssesmentVal)
    val teleOnlinevisitCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.modifierCodeVal)
    val joinedForteleOnlinevisitDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,teleOnlinevisitValueList,teleOnlinevisitCodeSystem)
    val mesrForteleOnlinevisitDf = UtilFunctions.dateBetweenFilter(joinedForteleOnlinevisitDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName)

    /*TeleHealth,Telephone Visits Value Set, Online Assessments Value Set*/
    val telemodtelonlineDf = mesurForTeleheamodDf.union(mesrForteleOnlinevisitDf)
    /*Members who Has outpatient valueset and TeleHealth,Telephone Visits Value Set, Online Assessments Value Set*/
    val outtelemodtelonlineDf = mesurForOutpatDf.as("df1").join(telemodtelonlineDf.as("df2"),$"sdf1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType).select("df1.*")

    /*iesd date column added to outpatient*/
    val iesddateAddedOutpatDf = outtelemodtelonlineDf.withColumn(KpiConstants.iesdDateColName,date_sub(outtelemodtelonlineDf.col(KpiConstants.startDateColName),KpiConstants.days60))

    val obsedwoInpatDf = mesurForObservEdDf.as("df1").join(edobsWoInpatDf.as("df2"),$"sdf1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType).select("df1.*")
    /*iesd date column added to Observation and Ed Df*/
    val iesddateAddedObservedDf = obsedwoInpatDf.withColumn(KpiConstants.iesdDateColName,date_sub(obsedwoInpatDf.col(KpiConstants.startDateColName),KpiConstants.days60))

    val iesdAddedUnionDf = iesddateAddedOutpatDf.union(iesddateAddedObservedDf)
    val iesdAddedAndFractDf = iesdAddedUnionDf.as("df1").join(fractureUnionDf.as("df2"),$"sdf1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType).select("df1.*")

    /*members who has a proceedure in the 60 days period prior to the iesd date*/
    val step2sub1Df = iesdAddedAndFractDf.as("df1").join(fractureUnionDf.as("df2"),$"sdf1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}" , KpiConstants.innerJoinType)
                                                          .filter(($"sdf2.${KpiConstants.startDateColName}".>=($"sdf1.${KpiConstants.iesdDateColName}")) &&($"sdf2.${KpiConstants.startDateColName}".<=($"sdf1.${KpiConstants.startDateColName}")))
                                                          .select("df1.member_sk")
    //</editor-fold>

    //<editor-fold desc="Step2 Sub2">

    val inPatStAndFractDf = mesurForInPatientStayDf.as("df1").join(fractureAndInpatDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"sdf2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                                    .select($"sdf1.${KpiConstants.memberskColName}", $"df1.${KpiConstants.admitDateColName}", $"df1.${KpiConstants.dischargeDateColName}")

    val iesdDateAddedinpatDf = inPatStAndFractDf.withColumn(KpiConstants.iesdDateColName,date_sub($"${KpiConstants.admitDateColName}",KpiConstants.days60))

    val step2Sub2Df = iesdDateAddedinpatDf.as("df1").join(iesdDateAddedinpatDf.select(KpiConstants.memberskColName,KpiConstants.dischargeDateColName).as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                           .filter(($"df2.${KpiConstants.dischargeDateColName}".>=($"df1.${KpiConstants.iesdDateColName}")) && ($"df2.${KpiConstants.dischargeDateColName}".<($"df1.${KpiConstants.admitDateColName}")))
                                                           .select($"df1.${KpiConstants.memberskColName}")
    //</editor-fold>

    val step2Df = step2sub1Df.union(step2Sub2Df)
    //</editor-fold>

    //<editor-fold desc="Step3">

    val contEnrolldateAddedDf = fractureUnionDf.withColumn(KpiConstants.contenrollLowCoName,date_sub(fractureUnionDf.col(KpiConstants.startDateColName),365)).withColumn(KpiConstants.contenrollUppCoName,date_add(fractureUnionDf.col(KpiConstants.startDateColName),180))
                                                .select(KpiConstants.memberskColName,KpiConstants.contenrollLowCoName,KpiConstants.contenrollUppCoName)

     val contEnrolledDf = contEnrolldateAddedDf.as("df1").join(genderFilterDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                               .filter(($"df2.${KpiConstants.memStartDateColName}".<=($"df1.${KpiConstants.contenrollLowCoName}")) && ($"df2.${KpiConstants.memEndDateColName}".>=($"df1.${KpiConstants.contenrollUppCoName}")))
                                               .select(s"df1.${KpiConstants.memberskColName}")

    val step3Df = contEnrolledDf
    //</editor-fold>

    //<editor-fold desc="Step4">

    //<editor-fold desc="Step4 Sub1(BMD test (Bone Mineral Density Tests Value Set) during the 730 days (24 months) prior to the IESD)">

    /*fractureAndOutObsEdDf as Dimmember(convert the start_date column to iesd_date)*/
    val fractureDf = fractureUnionDf.withColumnRenamed(KpiConstants.startDateColName,KpiConstants.iesdDateColName).select(KpiConstants.memberskColName,KpiConstants.iesdDateColName)

    val bmdTestValueSet = List(KpiConstants.boneMinDenTestVal)
    val bmdAsPrimDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,bmdTestValueSet,primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Getting the member_sk, start_date for the members who has done the BMD test as proceedure code and who has fracture and outpatient value*/
    val bmdCodeSystem = List(KpiConstants.cptCodeVal)
    val bmdAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,bmdTestValueSet,bmdCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val bmdDf = bmdAsPrimDiagDf.union(bmdAsProcDf)

    /*exclusion members (bmd test within 730 days period prior to the iesd date for the memberskds who has fracture and outpatient values)*/
    val fractureAndBmdTestDf = fractureDf.as("df1").join(bmdDf.as("df2"),fractureDf.col(KpiConstants.memberskColName) === bmdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                        .filter(datediff(fractureDf.col(KpiConstants.iesdDateColName),bmdDf.col(KpiConstants.startDateColName)).<=(KpiConstants.days730))
                                        .select(fractureDf.col(KpiConstants.memberskColName))
    //</editor-fold>

    //<editor-fold desc="Step4 Sub2(osteoporosis therapy (Osteoporosis Medications Value Set) within 365 days period prior to the iesd date for the memberskds who has fracture and outpatient values)">

    val omwOsteoprosisValueSet = List(KpiConstants.osteoporosisMedicationVal)
    val omwOsteoprosisCodeSystem = List(KpiConstants.hcpsCodeVal)
    val joinedForosteoprosisDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwOsteoprosisValueSet,omwOsteoprosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*exclusion members (osteoporosis therapy (Osteoporosis Medications Value Set) within 365 days period prior to the iesd date for the memberskds who has fracture and outpatient values)*/
    val fractureAndosteoprosisDf = fractureDf.as("df1").join(joinedForosteoprosisDf.as("df2"),fractureDf.col(KpiConstants.memberskColName) === joinedForosteoprosisDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
      .filter(datediff(fractureDf.col(KpiConstants.iesdDateColName),joinedForosteoprosisDf.col(KpiConstants.startDateColName)).<=(KpiConstants.days365))
      .select(fractureDf.col(KpiConstants.memberskColName))
    //</editor-fold>

    //<editor-fold desc="Step4 Sub3(Osteoprosis Medication) within 365 days prior to the iesd date.">

    val joinedForOstreoMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.omwMeasureId,omwOsteoprosisValueSet)
                                            .select(KpiConstants.memberskColName, KpiConstants.rxStartDateColName)

    val fractureAndOstreoMedDf = fractureDf.as("df1").join(joinedForOstreoMedDf.as("df2"),fractureDf.col(KpiConstants.memberskColName) === joinedForOstreoMedDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
      .filter(datediff(fractureDf.col(KpiConstants.iesdDateColName),joinedForOstreoMedDf.col(KpiConstants.rxStartDateColName)).<=(KpiConstants.days365))
      .select(fractureDf.col(KpiConstants.memberskColName))
    //</editor-fold>


    val step4Df = fractureAndBmdTestDf.union(fractureAndosteoprosisDf).union(fractureAndOstreoMedDf)
    //</editor-fold>

    //<editor-fold desc="Step5">

    /*Step5 Exclusion(Enrolled in an Institutional SNP (I-SNP)) starts*/
    /*Step5 Exclusion(Enrolled in an Institutional SNP (I-SNP)) ends*/

    //<editor-fold desc="81 years of age and older with frailty and advanced illness">

    /*Frality As Primary Diagnosis*/
    val fralityValList = List(KpiConstants.fralityVal)
    val joinedForFralityAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,fralityValList,primaryDiagCodeSystem)
    val measrForFralityAsDiagDf = UtilFunctions.measurementYearFilter(joinedForFralityAsDiagDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality As Proceedure Code*/
    val fralityCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForFralityAsProcDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,fralityValList,fralityCodeSystem)
    val measrForFralityAsProcDf = UtilFunctions.measurementYearFilter(joinedForFralityAsProcDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*Frality Union Data*/
    val fralityDf = measrForFralityAsDiagDf.union(measrForFralityAsProcDf)

    val age81OrMoreDf = UtilFunctions.ageFilter(genderFilterDf, KpiConstants.dobColName, year, KpiConstants.age81Val, KpiConstants.age85Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    /*(Members who has age 81 or more and has frailty (Frailty Value Set) )*/
    val fralityAbove81Df = age81OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityDf)
    //</editor-fold>

    //<editor-fold desc="(66 - 80 years of age and older with frailty and advanced illness)">

    /*(66 - 80 years of age and older with frailty and advanced illness) starts*/
    /*Advanced Illness valueset*/
    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,advillValList,primaryDiagCodeSystem)
    val measrForAdvancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*at least 2 Outpatient visit*/

    val joinedForTwoOutPatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,omwOutPatientValueSet,omwOutPatientCodeSystem)
    val measrForTwoOutPatDf = UtilFunctions.measurementYearFilter(joinedForTwoOutPatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoOutPatDf = measrForTwoOutPatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least 2 Observation visit*/
    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForTwoObservationDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,obsVisitValList,obsVisitCodeSystem)
    val measrForTwoObservationDf = UtilFunctions.measurementYearFilter(joinedForTwoObservationDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoObservationDf = measrForTwoObservationDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least ED visits*/
    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoEdVisistsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,edVisitValList,edVisitCodeSystem)
    val mesrForTwoEdVisitsDf = UtilFunctions.measurementYearFilter(joinedForTwoEdVisistsDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoEdVisitDf = mesrForTwoEdVisitsDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)

    /*at least two non acute inpatient*/
    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForTwoNonAcutePatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val mesrForTwoNonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForTwoNonAcutePatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName,KpiConstants.startDateColName)
    val twoNonAcutePatDf = mesrForTwoNonAcutePatDf.groupBy(KpiConstants.memberskColName).agg(countDistinct(KpiConstants.startDateColName).alias("countVal")).filter($"countVal".>=(2)).select(KpiConstants.memberskColName)
    val acuteInPatwoTeleDf = twoNonAcutePatDf

    /*Accute Inpatient*/
    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val measurementAcuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.startDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val).select(KpiConstants.memberskColName)

    /*union of atleast 2 outpatient visit, Observation visit,Ed visit,Non acute Visit*/
    val unionOfAllAtleastTwoVistDf = twoOutPatDf.union(twoObservationDf).union(twoEdVisitDf).union(twoNonAcutePatDf)

    /*Members who has atleast 2 visits in any of(outpatient visit, Observation visit,Ed visit,Non acute Visit) and advanced ill*/
    val advancedIllAndTwoVistsDf = unionOfAllAtleastTwoVistDf.intersect(measrForAdvancedIllDf)

    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set starts*/
    val acuteAndAdvancedIllDf = measurementAcuteInpatDf.intersect(measrForAdvancedIllDf)
    /*inpatient encounter (Acute Inpatient Value Set) with an advanced illness diagnosis (Advanced Illness Value Set ends*/

    /*dispensed dementia medication (Dementia Medications List) starts*/
    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val joinedForDemMedDf = UtilFunctions.factRxClaimRefMedValueSetJoinFunction(spark,factRxClaimsDf,ref_medvaluesetDf,KpiConstants.spdaMeasureId,dementiaMedValList)
    val MeasurementForDemMedDf = UtilFunctions.mesurementYearFilter(joinedForDemMedDf, KpiConstants.rxStartDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement2Val).select(KpiConstants.memberskColName)
    /*dispensed dementia medication (Dementia Medications List) ends*/

    /*Members who has advanced Ill*/
    val advancedIllDf = advancedIllAndTwoVistsDf.union(acuteAndAdvancedIllDf).union(MeasurementForDemMedDf)

    /*(Members who has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDf = fralityDf.intersect(advancedIllDf)

    val age66OrMoreDf = UtilFunctions.ageFilter(genderFilterDf, KpiConstants.dobColName, year, KpiConstants.age67Val, KpiConstants.age80Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    /*(Members who has age 66 or more and has frailty (Frailty Value Set) and Advanced Ill)*/
    val fralityAndAdvIlDfAndAbove65Df = age66OrMoreDf.select(KpiConstants.memberskColName).intersect(fralityAndAdvIlDf)
    /*(66 years of age and older with frailty and advanced illness) ends*/
    //</editor-fold>

    val step5Df = fralityAbove81Df.union(fralityAndAdvIlDfAndAbove65Df)
    //</editor-fold>

    val dinoIncludeDf = step1Df.union(step3Df)
    val dinoExclDf = step2Df.union(step4Df).union(step5Df)
    val finalDinominatorDf = dinoIncludeDf.except(dinoExclDf)

    val dinominatorDf = genderFilterDf.as("df1").join(finalDinominatorDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}" , KpiConstants.innerJoinType)
                                      .select("df1.*")

    val dinominatorForKpiDf = dinominatorDf.select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*(Hospice Exclusion) starts*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf)
    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName)
    val dinoAfterExclDf = dinominatorForKpiDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">


    /*Numerator1(BMD test (Bone Mineral Density Tests Value Set) for outpatient during 180 days after IESD date)*/
    val numBmdTestForOutPatDf = fractureUnionDf.as("df1").join(bmdDf.as("df2"),fractureUnionDf.col(KpiConstants.memberskColName) === bmdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                               .filter(datediff(bmdDf.col(KpiConstants.startDateColName),fractureUnionDf.col(KpiConstants.startDateColName)).<=(180))
                                               .select(fractureUnionDf.col(KpiConstants.memberskColName))

    /*Numerator2(BMD test (Bone Mineral Density Tests Value Set) for Inpatient during 180 days after IESD date)*/
    val numBmdTestForInPatDf =  iesdDateAddedinpatDf.as("df1").join(bmdDf.as("df2"),iesdDateAddedinpatDf.col(KpiConstants.memberskColName) === bmdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                                    .filter(datediff(bmdDf.col(KpiConstants.startDateColName),iesdDateAddedinpatDf.col(KpiConstants.admitDateColName)).>=(0) &&  datediff(iesdDateAddedinpatDf.col(KpiConstants.dischargeDateColName),bmdDf.col(KpiConstants.startDateColName)).>=(0))
                                                    .select(iesdDateAddedinpatDf.col(KpiConstants.memberskColName))

    /*Numerator3 (Osteoporosis therapy (Osteoporosis Medications Value Set) for fracture during 180 days after Iesd date)*/
    val numOsteoTestForOutPatDf = fractureUnionDf.as("df1").join(joinedForosteoprosisDf.as("df2"),fractureUnionDf.col(KpiConstants.memberskColName) === joinedForosteoprosisDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                                 .filter(datediff(joinedForosteoprosisDf.col(KpiConstants.startDateColName),fractureUnionDf.col(KpiConstants.startDateColName)).<=(KpiConstants.days180))
                                                 .select(fractureUnionDf.col(KpiConstants.memberskColName))


    /*Numerator4(Osteoporosis therapy (Osteoporosis Medications Value Set) for Inpatient during 180 days after IESD date)*/
    val numOsteoTestForForInPatDf =  iesdDateAddedinpatDf.as("df1").join(joinedForosteoprosisDf.as("df2"),iesdDateAddedinpatDf.col(KpiConstants.memberskColName) === joinedForosteoprosisDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
      .filter(datediff(joinedForosteoprosisDf.col(KpiConstants.startDateColName),iesdDateAddedinpatDf.col(KpiConstants.admitDateColName)).>=(0) &&  datediff(iesdDateAddedinpatDf.col(KpiConstants.dischargeDateColName),joinedForosteoprosisDf.col(KpiConstants.startDateColName)).>=(0))
      .select(iesdDateAddedinpatDf.col(KpiConstants.memberskColName))

    /*Numerator 5(Osteoporsis Medication during 180 days after the iesd date)*/
    val numfractureAndOstreoMedDf = fractureDf.as("df1").join(joinedForOstreoMedDf.as("df2"),fractureDf.col(KpiConstants.memberskColName) === joinedForOstreoMedDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
      .filter(datediff(joinedForOstreoMedDf.col(KpiConstants.rxStartDateColName),fractureDf.col(KpiConstants.iesdDateColName)).<=(KpiConstants.days180))
      .select(fractureDf.col(KpiConstants.memberskColName))

    val numUnionDf = numBmdTestForOutPatDf.union(numBmdTestForInPatDf).union(numOsteoTestForOutPatDf).union(numOsteoTestForForInPatDf).union(numfractureAndOstreoMedDf)

    val numeratorDf = numUnionDf.intersect(dinoAfterExclDf)
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*create the reason valueset for output data*/
    val numeratorValueSet = bmdTestValueSet:::omwOsteoprosisValueSet:::omwOutPatientValueSet
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




