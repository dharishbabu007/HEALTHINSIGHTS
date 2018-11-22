package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._



object NcqaOMW {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAOMW")
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
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.omwMeasureTitle)
    /*Age & Gender filter*/
    val ageFilterDf = UtilFunctions.ageFilter(initialJoinedDf,KpiConstants.dobColName,year,KpiConstants.age67Val,KpiConstants.age85Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    val genderFilter = ageFilterDf.filter($"gender".===("F"))




    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)



    //<editor-fold desc="Dinominator Calculation">
    /*Dinominator Calculation starts*/

    /*intake dates calculation*/
    val firstIntakeDate = year.toInt-1+"-01-01"
    val secondIntakeDate = year+"-06-30"



    //<editor-fold desc="Dinominator1">
    /*dinominator1 starts*/

    /*outpatient visit (Outpatient Value Set), an observation visit (Observation Value Set) or an ED visit (ED Value Set)*/
    val hedisJoinedForDinominator1Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwOutPatientValueSet,KpiConstants.omwOutPatientCodeSystem)
    val mesurementFilterForOutpatientDf = UtilFunctions.dateBetweenFilter(hedisJoinedForDinominator1Df,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate)


    /*fracture (Fractures Value Set) AS Primary Diagnosis*/
    val hedisJoinedForFractureAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwFractureValueSet,KpiConstants.primaryDiagnosisCodeSystem)
    val mesurementFilterForFractureAsDiagDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsDiagDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName)


    /*fracture (Fractures Value Set) AS Procedure Code*/
    val hedisJoinedForFractureAsProDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwFractureValueSet,KpiConstants.omwFractureCodeSystem)
    val mesurementFilterForFractureAsProDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsProDf,KpiConstants.startDateColName,firstIntakeDate,secondIntakeDate).select(KpiConstants.memberskColName)

    /*Fracture Dinominator (Union of Fractures Value Set AS Primary Diagnosis and Fractures Value Set AS Procedure Code)*/
    val fractureUnionDf = mesurementFilterForFractureAsDiagDf.union(mesurementFilterForFractureAsProDf)

    /*Mmebers who has fracture and any of the values(Outpatient,Observation,ED)*/
    val fractureAndOutObsEdDf = mesurementFilterForOutpatientDf.select("*").as("df1").join(fractureUnionDf.as("df2"),mesurementFilterForOutpatientDf.col(KpiConstants.memberskColName) === fractureUnionDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    //fractureAndOutObsEdDf.printSchema()
    val dinominatorOne_SuboneDf = fractureAndOutObsEdDf.select(KpiConstants.memberskColName)


    /*Second Sub Condition for the First Dinominator*/
    val hedisJoinedForInpatientStDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dimMemberDf,factClaimDf,refHedisDf,"procedure_code","inner",KpiConstants.omwMeasureId,KpiConstants.omwInpatientStayValueSet,KpiConstants.omwInpatientStayCodeSystem)
    val mesurementFilterForInPatientStayDf = UtilFunctions.dateBetweenFilter(hedisJoinedForFractureAsProDf,KpiConstants.dischargeDateColName,firstIntakeDate,secondIntakeDate)

    /*Members who has fracture and acute or nonacute inpatient valueset*/
    val fractureAndInpatientDf = mesurementFilterForInPatientStayDf.select("*").as("df1").join(fractureUnionDf.as("df2"),mesurementFilterForInPatientStayDf.col(KpiConstants.memberskColName) === fractureUnionDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinominatorOne_SubTwoDf = fractureAndInpatientDf.select(KpiConstants.memberskColName)


    /*Dinominator1 (union of dinominatorOne_SuboneDf and dinominatorOne_SubTwoDf)*/
    val omwDinominatorOneDf = dinominatorOne_SuboneDf.union(dinominatorOne_SubTwoDf).distinct()
    /*dinominator1 ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator2">
    /*Dinominator2(Step2)(Negative Diagnosis History. Exclude members who had either of the following during the 60-day (2 months) period prior to the IESD ) Starts*/

    /*Dinominator2 sub1 (for outpatient)*/
    /*iesd date column added to the fractureAndOutObsEdDf*/
    val iesdAddedForfractureAndOutObsEdDf = fractureAndOutObsEdDf.withColumn(KpiConstants.iesdDateColName,date_sub(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),60))

    /*join iesdAddedForfractureAndOutObsEdDf with fractureAndOutObsEdDf based on member_sk and filter out who has a history in the last 60 days period*/
    val fractureAndOutObsEdWnhistoryDf = iesdAddedForfractureAndOutObsEdDf.as("df1").join(fractureAndOutObsEdDf.as("df2"),iesdAddedForfractureAndOutObsEdDf.col(KpiConstants.memberskColName) === fractureAndOutObsEdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(fractureAndOutObsEdDf.col(KpiConstants.startDateColName).>=(iesdAddedForfractureAndOutObsEdDf.col(KpiConstants.iesdDateColName)) && fractureAndOutObsEdDf.col(KpiConstants.startDateColName).<=(iesdAddedForfractureAndOutObsEdDf.col(KpiConstants.startDateColName))).select("df1.*")
    val dinominatorSecond_SubOneDf = fractureAndOutObsEdWnhistoryDf.select(KpiConstants.memberskColName)


    /*Dinominator2 sub2 (for inpatient)*/
    /*iesd date column added to the fractureAndInpatientDf*/
    val iesAddedForfractureAndInpatientDf = fractureAndInpatientDf.withColumn(KpiConstants.iesdDateColName,date_sub(fractureAndInpatientDf.col(KpiConstants.dischargeDateColName),60))

    /*join iesAddedForfractureAndInpatientDf with fractureAndInpatientDf based on member_sk and filter out who has a history in the last 60 days period*/
    val fractureAndInpatientWnhistoryDf = iesAddedForfractureAndInpatientDf.as("df1").join(fractureAndInpatientDf.as("df2"),iesAddedForfractureAndInpatientDf.col(KpiConstants.memberskColName) === fractureAndInpatientDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(fractureAndInpatientDf.col(KpiConstants.startDateColName).>=(iesAddedForfractureAndInpatientDf.col(KpiConstants.iesdDateColName)) && fractureAndInpatientDf.col(KpiConstants.startDateColName).<=(iesAddedForfractureAndInpatientDf.col(KpiConstants.dischargeDateColName))).select("df1.*")
    val dinominatorSecond_SubtwoDf = fractureAndInpatientWnhistoryDf.select(KpiConstants.memberskColName)

    /*Dinominator2(Negative History Mmebersks(who shas to exclude from Dinominator))*/
    val dinominator2Df = dinominatorSecond_SubOneDf.union(dinominatorSecond_SubtwoDf)
    /*Dinominator2(Step2) Ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator3">
    /*Dinominator3(Members who continuously enrolled during the 12 months prior to the fracture through 180 days (6 months) post-fracture)*/

    /*member_sk who has Outpatient and Fracture*/
    val datesAddedForFractureAndOutPatDf = fractureAndOutObsEdDf.withColumn("continuous_Enroll_LowerDate",date_sub(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),365)).withColumn("continuous_Enroll_UpperDate",date_add(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),180)).
                                                                  select(KpiConstants.memberskColName,"continuous_Enroll_LowerDate","continuous_Enroll_UpperDate")

    /*join with fact_membership for getting the membership start date and end date sks*/
    val factMembershipAddedDf = datesAddedForFractureAndOutPatDf.as("df1").join(factMembershipDf.as("df2"),datesAddedForFractureAndOutPatDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*","df2.member_plan_start_date_sk","df2.member_plan_end_date_sk")
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val factMembershipAddedstDateDf = factMembershipAddedDf.as("df1").join(dimDateDf.as("df2"),factMembershipAddedDf.col("member_plan_start_date_sk") === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select("df1.*","df2.calender_date").withColumnRenamed("calender_date","start_date_temp")
    val factMembershipAddedendDateDf = factMembershipAddedstDateDf.as("df1").join(dimDateDf.as("df2"),factMembershipAddedstDateDf.col("member_plan_end_date_sk") === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select("df1.*","df2.calender_date").withColumnRenamed("calender_date","end_date_temp")
    val dateFormaatedDf = factMembershipAddedendDateDf.withColumn("start_date",to_date($"start_date_temp", "dd-MMM-yyyy")).withColumn("end_date",to_date($"end_date_temp", "dd-MMM-yyyy"))

    /*Elements who is continusly enrolled in membership start date and end date*/
    val fractureAndOutPatContDf = dateFormaatedDf.filter(datediff($"start_date",$"continuous_Enroll_LowerDate").>=(0) && datediff($"end_date",$"continuous_Enroll_UpperDate").>=(0)).select(KpiConstants.memberskColName)

    /*member_sk who has Inpatient and Fracture*/
    val  datesAddedForfractureAndInpatientDfDf = fractureAndInpatientDf.withColumn("continuous_Enroll_LowerDate",date_sub(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),365)).withColumn("continuous_Enroll_UpperDate",date_add(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),180)).
                                                                        select(KpiConstants.memberskColName,"continuous_Enroll_LowerDate","continuous_Enroll_UpperDate")


    val factMembershipAddedForInpatDf = datesAddedForfractureAndInpatientDfDf.as("df1").join(factMembershipDf.as("df2"),datesAddedForfractureAndInpatientDfDf.col(KpiConstants.memberskColName) === factMembershipDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*","df2.member_plan_start_date_sk","df2.member_plan_end_date_sk")
    val factMembershipAddedstDateForInpatDf = factMembershipAddedForInpatDf.as("df1").join(dimDateDf.as("df2"),factMembershipAddedForInpatDf.col("member_plan_start_date_sk") === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select("df1.*","df2.calender_date").withColumnRenamed("calender_date","start_date_temp")
    val factMembershipAddedendDateForInpatDf = factMembershipAddedstDateForInpatDf.as("df1").join(dimDateDf.as("df2"),factMembershipAddedstDateForInpatDf.col("member_plan_end_date_sk") === dimDateDf.col(KpiConstants.dateSkColName),KpiConstants.innerJoinType).select("df1.*","df2.calender_date").withColumnRenamed("calender_date","end_date_temp")
    val dateFormaatedForInpatDf = factMembershipAddedendDateForInpatDf.withColumn("start_date",to_date($"start_date_temp", "dd-MMM-yyyy")).withColumn("end_date",to_date($"end_date_temp", "dd-MMM-yyyy"))

    /*Elements who is continusly enrolled in membership start date and end date*/
    val fractureAndInpatContDf = dateFormaatedDf.filter(datediff($"start_date",$"continuous_Enroll_LowerDate").>=(0) && datediff($"end_date",$"continuous_Enroll_UpperDate").>=(0)).select(KpiConstants.memberskColName)

    val dino3Df = fractureAndOutPatContDf.union(fractureAndInpatContDf)
    /*Dinominator3 ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator4">
    /*Dinominator4(Step4) (Required Exclusions starts)*/


    //<editor-fold desc="Dinominator4Exclusion1">
    /*Dinominator Exclusion1(BMD test (Bone Mineral Density Tests Value Set) during the 730 days (24 months) prior to the IESD)*/

    /*fractureAndOutObsEdDf as Dimmember(convert the start_date column to iesd_date)*/
    val fractureAndOutObsEdAsDimMemberDf = fractureAndOutObsEdDf.withColumnRenamed(KpiConstants.startDateColName,KpiConstants.iesdDateColName).select(KpiConstants.memberskColName,KpiConstants.iesdDateColName)

    /*Getting the member_sk, start_date for the members who has done the BMD test as primary Diagnosis and who has fracture and outpatient value*/
    val bmdAsPrimDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndOutObsEdAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwBmdTestValueSet,KpiConstants.primaryDiagnosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Getting the member_sk, start_date for the members who has done the BMD test as proceedure code and who has fracture and outpatient value*/
    val bmdAsProcDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndOutObsEdAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwBmdTestValueSet,KpiConstants.omwBmdTestCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*bmdtest values (union of bmd as primary diagnosis and proceddure code)*/
    val bmdvalDf = bmdAsPrimDiagDf.union(bmdAsProcDf)

    /*exclusion members (bmd test within 730 days period prior to the iesd date for the memberskds who has fracture and outpatient values)*/
    val outPatientBmdTestDf = fractureAndOutObsEdAsDimMemberDf.as("df1").join(bmdvalDf.as("df2"),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName) === bmdvalDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.iesdDateColName),bmdvalDf.col(KpiConstants.startDateColName)).<=(730)).select(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName))


    /*fractureAndInpatientDf as DimMember(convert the discharge_date column to the iesd_date)*/
    val fractureAndInpatientAsDimMemberDf = fractureAndInpatientDf.withColumnRenamed(KpiConstants.dischargeDateColName,KpiConstants.iesdDateColName).select(KpiConstants.memberskColName,KpiConstants.iesdDateColName,KpiConstants.admitDateColName)

    /*Getting the member_sk, start_date for the members who has done the BMD test as primary Diagnosis and who has fracture and inpatient value*/
    val bmdAsPrimDiagForFraAndInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndInpatientAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwBmdTestValueSet,KpiConstants.primaryDiagnosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Getting the member_sk, start_date for the members who has done the BMD test as proceedure code and who has fracture and inpatient value*/
    val bmdAsProcForFraAndInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndInpatientAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwBmdTestValueSet,KpiConstants.omwBmdTestCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*bmdtest values (union of bmd as primary diagnosis and proceddure code)*/
    val bmdvalForFraAndInpatDf = bmdAsPrimDiagForFraAndInpatDf.union(bmdAsProcForFraAndInpatDf)

    /*exclusion members (bmd test within 730 days period prior to the iesd date for the memberskds who has fracture and inpatient values)*/
    val inpatientBmdTestDf = fractureAndInpatientAsDimMemberDf.as("df1").join(bmdvalForFraAndInpatDf.as("df2"),fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName) === bmdvalForFraAndInpatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(fractureAndInpatientAsDimMemberDf.col(KpiConstants.iesdDateColName),bmdvalForFraAndInpatDf.col(KpiConstants.startDateColName)).<=(730)).select(fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName))

    /*Dinominator Exclusion 1 (members who has done BMD test in the period of 730 days prior to the iesd date)*/
    val dinoExcl1Df = outPatientBmdTestDf.union(inpatientBmdTestDf)
    //</editor-fold>

    //<editor-fold desc="Dinominator4 Exclusion2">
    /*Dinominator Exclusion2(osteoporosis therapy (Osteoporosis Medications Value Set) during the 365 days (12 months) prior to the IESD)*/

    /*Getting the member_sk, start_date for the members who has done the osteoporosis therapy and who has fracture and outpatient value*/
    val osteoForFractureAndOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndOutObsEdAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwOsteoprosisValueSet,KpiConstants.omwOsteoprosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*exclusion members (osteoporosis therapy (Osteoporosis Medications Value Set) within 365 days period prior to the iesd date for the memberskds who has fracture and outpatient values)*/
    val osteoValForFraAndOutPatDf = fractureAndOutObsEdAsDimMemberDf.as("df1").join(osteoForFractureAndOutPatDf.as("df2"),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName) === osteoForFractureAndOutPatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.iesdDateColName),osteoForFractureAndOutPatDf.col(KpiConstants.startDateColName)).<=(365)).select(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName))


    /*Getting the member_sk, start_date for the members who has done the osteoporosis therapy and who has fracture and Inpatient value*/
    val osteoForFractureAndInPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndInpatientAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwOsteoprosisValueSet,KpiConstants.omwOsteoprosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*exclusion members (osteoporosis therapy (Osteoporosis Medications Value Set) within 365 days period prior to the iesd date for the memberskds who has fracture and inpatient values)*/
    val osteoValForFraAndInPatDf = fractureAndInpatientAsDimMemberDf.as("df1").join(osteoForFractureAndInPatDf.as("df2"),fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName) === osteoForFractureAndInPatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(fractureAndInpatientAsDimMemberDf.col(KpiConstants.iesdDateColName),osteoForFractureAndInPatDf.col(KpiConstants.startDateColName)).<=(365)).select(fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName))


    /*Dinominator Exclusion 2 (members who has done osteoporosis therapy in the period of 365 days prior to the iesd date)*/
    val dinoExcl2Df = osteoValForFraAndOutPatDf.union(osteoValForFraAndInPatDf)
    //</editor-fold>

    /*Dinominator3Exclusion starts*/
    /*Dinominator3Exclusion ends*/

    val dinominator4UnionDf = dinoExcl1Df.union(dinoExcl2Df)
    //</editor-fold>



    val dinominatorUnionDf = omwDinominatorOneDf.union(dino3Df)
    val exclUnionDf = dinominator2Df.union(dinominator4UnionDf)
    val finalDinoDf = dinominatorUnionDf.except(exclUnionDf)
    val dinominatorDf = genderFilter.as("df1").join(finalDinoDf.as("df2"),genderFilter.col(KpiConstants.memberskColName) === finalDinoDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    val dinoForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)

    /*Dinominator Calculation Ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion">

    /*Dinominator Exclusion Calculation starts*/
    /*Dinominator Exclusion1(Hospice Exclusion) starts*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    val dinominatorExclDf = hospiceDf.select(KpiConstants.memberskColName)
    val dinoAfterExclDf = dinoForKpiCalDf.except(dinominatorExclDf)
    /*Dinominator Exclusion3(Hospice Exclusion) ends*/
    /*Dinominator Exclusion Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator">
    /*Numerator Starts*/

    /*Numerator1(BMD test (Bone Mineral Density Tests Value Set) for outpatient during 180 days after IESD date)*/
    val numBmdTestForOutPatDf = fractureAndOutObsEdAsDimMemberDf.as("df1").join(bmdvalDf.as("df2"),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName) === bmdvalDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(bmdvalDf.col(KpiConstants.startDateColName),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.iesdDateColName)).<=(180)).select(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName))

    /*Numerator2(BMD test (Bone Mineral Density Tests Value Set) for Inpatient during 180 days after IESD date)*/
    val numBmdTestForInPatDf =  fractureAndInpatientAsDimMemberDf.as("df1").join(bmdvalForFraAndInpatDf.as("df2"),fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName) === bmdvalForFraAndInpatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(bmdvalForFraAndInpatDf.col(KpiConstants.startDateColName),fractureAndInpatientAsDimMemberDf.col(KpiConstants.admitDateColName)).>=(0) &&  datediff(fractureAndInpatientAsDimMemberDf.col(KpiConstants.iesdDateColName),bmdvalForFraAndInpatDf.col(KpiConstants.startDateColName)).>=(0)).select(fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName))

    /*Numerator3 (Osteoporosis therapy (Osteoporosis Medications Value Set) for outpatient during 180 days after Iesd date)*/
    val numOsteoTestForOutPatDf = fractureAndOutObsEdAsDimMemberDf.as("df1").join(osteoForFractureAndOutPatDf.as("df2"),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName) === osteoForFractureAndOutPatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(osteoForFractureAndOutPatDf.col(KpiConstants.startDateColName),fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.iesdDateColName)).<=(180)).select(fractureAndOutObsEdAsDimMemberDf.col(KpiConstants.memberskColName))


    /*Getting the member_sk, start_date for the members who has done the Long osteoporosis therapy and who has fracture and Inpatient value*/
    val longosteoForFractureAndInPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,fractureAndInpatientAsDimMemberDf,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.omwMeasureId,KpiConstants.omwLongOsteoprosisValueSet,KpiConstants.omwOsteoprosisCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)


    /*Numerator4 (Osteoporosis therapy (Osteoporosis Medications Value Set) for inpatient during 180 days after Iesd date)*/
    val numOsteoTestForInPatDf = fractureAndInpatientAsDimMemberDf.as("df1").join(longosteoForFractureAndInPatDf.as("df2"),fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName) === osteoForFractureAndInPatDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff(longosteoForFractureAndInPatDf.col(KpiConstants.startDateColName),fractureAndInpatientAsDimMemberDf.col(KpiConstants.admitDateColName)).>=(0) &&   datediff(fractureAndInpatientAsDimMemberDf.col(KpiConstants.iesdDateColName),longosteoForFractureAndInPatDf.col(KpiConstants.startDateColName)).>=(0)).select(fractureAndInpatientAsDimMemberDf.col(KpiConstants.memberskColName))

    val numeratorUnionDf = numBmdTestForOutPatDf.union(numBmdTestForInPatDf).union(numOsteoTestForOutPatDf).union(numOsteoTestForInPatDf)

    val numeratorDf = numeratorUnionDf.intersect(dinoAfterExclDf)
    /*Numerator Ends*/
    //</editor-fold>

    //<editor-fold desc="output to fact_hedis_gaps_in_care">

    /*Common output format (data to fact_hedis_gaps_in_care) starts*/
    /*create the reason valueset for output data*/
    val numeratorValueSet = KpiConstants.omwBmdTestValueSet:::KpiConstants.omwOsteoprosisValueSet:::KpiConstants.omwOutPatientValueSet
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,KpiConstants.omwMeasureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.format("parquet").mode(SaveMode.Append).insertInto(KpiConstants.dbName+"."+KpiConstants.outGapsInHedisTestTblName)
    /*Common output format (data to fact_hedis_gaps_in_care) ends*/
    //</editor-fold>

    //<editor-fold desc="Output to fact_hedis_qms starts">

    /*Data populating to fact_hedis_qms starts*/
    val qualityMeasureSk = DataLoadFunctions.qualityMeasureLoadFunction(spark, KpiConstants.omwMeasureTitle).select("quality_measure_sk").as[String].collectAsList()(0)
    val factMembershipDfForoutDf = factMembershipDf.select("member_sk", "lob_id")
    val outFormattedDf = UtilFunctions.outputCreationForHedisQmsTable(spark, factMembershipDfForoutDf, qualityMeasureSk, data_source)
    outFormattedDf.write.mode(SaveMode.Overwrite).saveAsTable("ncqa_sample.fact_hedis_qms")
    /*Data populating to fact_hedis_qms ends*/
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
