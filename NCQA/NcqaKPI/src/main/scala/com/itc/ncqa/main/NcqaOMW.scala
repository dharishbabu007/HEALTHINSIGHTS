package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{abs, concat, current_timestamp, date_add, date_format, datediff, expr, lit, to_date, when,date_sub}
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
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.abaMeasureTitle)
    //initialJoinedDf.show(50)
    //lookupTableDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    /*common filter checking*/
    //val commonFilterDf = initialJoinedDf.as("df1").join(lookupTableDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookupTableDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookupTableDf.col(KpiConstants.startDateColName).isNull).select("df1.*")




    /*Age & Gender filter*/
    val ageFilterDf = UtilFunctions.ageFilter(initialJoinedDf,KpiConstants.dobColName,year,KpiConstants.age67Val,KpiConstants.age85Val,KpiConstants.boolTrueVal,KpiConstants.boolTrueVal)
    val genderFilter = ageFilterDf.filter($"gender".===("F"))



    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)


    /*Dinominator Calculation starts*/
    /*intake dates calculation*/
    val firstIntakeDate = year.toInt-1+"-01-01"
    val secondIntakeDate = year+"-06-30"

    /*Dinominator1 Starts*/
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
    /*Dinominator1 Ends*/


    /*Dinominator2(Negative Diagnosis History. Exclude members who had either of the following during the 60-day (2 months) period prior to the IESD ) Starts*/

    /*Dinominator2 sub1 (for outpatient)*/
    /*iesd date column added to the fractureAndOutObsEdDf*/
    val iesdAddedForfractureAndOutObsEdDf = fractureAndOutObsEdDf.withColumn("iesd_date",date_sub(fractureAndOutObsEdDf.col(KpiConstants.startDateColName),60))

    /*join iesdAddedForfractureAndOutObsEdDf with fractureAndOutObsEdDf based on member_sk and filter out who has a history in the last 60 days period*/
    val fractureAndOutObsEdDfWNhistoryDf = iesdAddedForfractureAndOutObsEdDf.as("df1").join(fractureAndOutObsEdDf.as("df2"),iesdAddedForfractureAndOutObsEdDf.col(KpiConstants.memberskColName) === fractureAndOutObsEdDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter($"df2.start_date".>=($"df1.iesd_date") && $"df2.start_date".<=($"df1.start_date")).select("df1.*")
    val dinominatorSecond_SubOneDf = fractureAndOutObsEdDfWNhistoryDf.select(KpiConstants.memberskColName)

    /*Dinominator2 Ends*/


  }
}
