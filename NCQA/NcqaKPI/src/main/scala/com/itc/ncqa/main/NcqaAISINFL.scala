package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{datediff, lit}
import org.apache.spark.sql.types.DateType

object NcqaAISINFL {

  def main(args: Array[String]): Unit = {


    //<editor-fold desc="Reading program arguments and SaprkSession oBject creation">

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAISFL")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val factRxClaimsDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)
    //</editor-fold>

    //<editor-fold desc="Initial join, age filter, age filter for numerator">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.aisMeasureTitle)
    //initialJoinedDf.show(50)

    var current_date = year + "-01-01"
    val newDf1 = initialJoinedDf.withColumn("curr_date", lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))

    /*Age filter for dinominator calculation*/
    val ageFilterDf = newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).>=(19)).drop("curr_date")

    /*Age filter for numerator based on the lob_name*/
    var ageFilterForNumeratorDf = spark.emptyDataFrame

    if(KpiConstants.aisf1MeasureId.equalsIgnoreCase(measureId)){
      ageFilterForNumeratorDf = newDf2.filter(((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).>=(19)) && ((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).<=(66))).drop("curr_date").select(KpiConstants.memberskColName)
    }
    else{
      ageFilterForNumeratorDf = newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).>(66)).drop("curr_date").select(KpiConstants.memberskColName)
    }
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    /*Dinominator Calculation starts*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)

    //<editor-fold desc="Dinominator Exclusion Calculation">

    /*Dinominator Exclsuion starts*/

    //<editor-fold desc="Dinominator Exclusion1">

    /*Dinominator Exclusion1 (Anaphylactic Reaction)Starts*/
    val ardvValList = List(KpiConstants.ardvVal)
    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val joinedForardvDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,ardvValList,primaryDiagCodeSystem).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion1 (Anaphylactic Reaction)Ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion2">

    /*Dinominator Exclusion2 (Encephalopathy within 7 days after tdap vaccine) starts*/
    /*Tdap Vaccine Membersks*/
    val tdapVaccineValList = List(KpiConstants.tdapVaccineVal)
    val joinedForTdapVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,tdapVaccineValList,primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    /*Encephalopathy*/
    val encephalopathyValList = List(KpiConstants.encephalopathyVal)
    val joinedForencephalopathyDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,encephalopathyValList,primaryDiagCodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val encephalopathyAfterTdapDf = joinedForTdapVacDf.as("df1").join(joinedForencephalopathyDf.as("df2"),joinedForTdapVacDf.col(KpiConstants.memberskColName) === joinedForencephalopathyDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                    .filter(datediff(joinedForencephalopathyDf.col(KpiConstants.startDateColName),joinedForTdapVacDf.col(KpiConstants.startDateColName)).<=(7)).select(joinedForencephalopathyDf.col(KpiConstants.memberskColName))
    /*Dinominator Exclusion2 (Encephalopathy within 7 days after tdap vaccine) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion3">

    /*Dinominator Exclsuion3(Active chemotherapy) during measurement period starts*/
    val activeChemValList = List(KpiConstants.chemoTherappyVal)
    val joinedForActiveChemoDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,activeChemValList,primaryDiagCodeSystem)
    val measureemtnForActChemoDf = UtilFunctions.mesurementYearFilter(joinedForActiveChemoDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    /*Dinominator Exclsuion3(Active chemotherapy) during measurement period ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion4">

    /*Dinominator Exclusion4(Bone Marrow Transplant) during the measurement period starts*/
    val bmtValList = List(KpiConstants.boneMarowTransVal)
    val joinedForbmtDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,bmtValList,primaryDiagCodeSystem)
    val measurementForbmtDf = UtilFunctions.mesurementYearFilter(joinedForbmtDf,KpiConstants.startDateColName,year,KpiConstants.measurementYearLower,KpiConstants.measurementOneyearUpper).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion4(Bone Marrow Transplant) during the measurement period ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion5">

    /*Dinominator Exclusion5 (immunocompromising conditions,cochlear implants,anatomic or functional asplenia,sickle cell anemia & HB-S disease or cerebrospinal fluid leaks) starts*/
    val valList = List(KpiConstants.immunoCompromisingVal,KpiConstants.cochlearImplantVal,KpiConstants.afaVal,KpiConstants.scaHbsdVal,KpiConstants.cflVal)
    val joinedForDinoExcl5df = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,valList,primaryDiagCodeSystem).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion5 (immunocompromising conditions,cochlear implants,anatomic or functional asplenia,sickle cell anemia & HB-S disease or cerebrospinal fluid leaks) ends*/
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion6">

    /*Dinominator Exclusion6(Hospice) starts*/
    val hospiceDf = UtilFunctions.hospiceMemberDfFunction(spark,dimMemberDf,factClaimDf,refHedisDf)
    val measurementForHospiceDf = UtilFunctions.mesurementYearFilter(hospiceDf,KpiConstants.startDateColName,year,0,365).select(KpiConstants.memberskColName)
    /*Dinominator Exclusion6(Hospice) ends*/
    //</editor-fold>

    val dinoExclDf = joinedForardvDf.union(encephalopathyAfterTdapDf).union(measureemtnForActChemoDf).union(measurementForbmtDf).union(joinedForDinoExcl5df).union(measurementForHospiceDf)
    /*Dinominator Exclsuion ends*/
    //</editor-fold>

    val dinominatorMemSkDf = ageFilterDf.select(KpiConstants.memberskColName).except(dinoExclDf)
    /*Dinominator calculation*/
    val dinominatorDf = ageFilterDf.as("df1").join(dinominatorMemSkDf.as("df2"),ageFilterDf.col(KpiConstants.memberskColName) === dinominatorMemSkDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).select("df1.*")
    /*Dinominator Calculation starts*/
    //</editor-fold>

    //<editor-fold desc="Numerator calculation">

    /*Numerator Calculation(members who are in dinominator and have influenza vaccine in between the dates that are given) starts*/
    val influenzaVaccineValList = List(KpiConstants.influenzaVaccineVal)
    val influenzaVacCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal,KpiConstants.hcpsCodeVal)
    val joinedForInfluenzaVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,influenzaVaccineValList,influenzaVacCodeSystem)
    val lowerDate = year.toInt-1 + "-07-01"
    val upperDate = year + "-06-30"
    val measureForInfluenzaVaccDf = UtilFunctions.dateBetweenFilter(joinedForInfluenzaVacDf,KpiConstants.startDateColName,lowerDate,upperDate).select(KpiConstants.memberskColName)
    /*numerator based on the lob name*/
    val numAgeFilterDf = measureForInfluenzaVaccDf.intersect(ageFilterForNumeratorDf)
    /*Members who are in dinominator and have a influenza vacccine in between the given dates*/
    val numeratorDf = numAgeFilterDf.intersect(dinominatorMemSkDf)
    /*Numerator Calculation(members who are in dinominator and have influenza vaccine in between the dates that are given) ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = influenzaVaccineValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)

    val numExclDf = spark.emptyDataFrame
    val dinominatorExclDf = spark.emptyDataFrame

    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>

    spark.sparkContext.stop()


  }
}
