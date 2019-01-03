package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object NcqaAISPNEU2 {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaAISPNEU2")
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

    //<editor-fold desc="Initial Join, Age filter">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.aisMeasureTitle)
    //initialJoinedDf.show(50)

    var current_date = year + "-01-01"
    val newDf1 = initialJoinedDf.withColumn("curr_date", lit(current_date))
    val newDf2 = newDf1.withColumn("curr_date", newDf1.col("curr_date").cast(DateType))
    /*Age filter for dinominator calculation*/
    val ageFilterDf = newDf2.filter((datediff(newDf2.col("curr_date"), newDf2.col(KpiConstants.dobColName)) / 365.25).>=(66)).drop("curr_date")
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
    /*Dinominator Calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator calculation starts*/
    val memDf = ageFilterDf.withColumn(KpiConstants.sixtyDobColName,add_months(ageFilterDf.col(KpiConstants.dobColName),720)).select(KpiConstants.memberskColName,KpiConstants.dobColName,KpiConstants.sixtyDobColName)
    /*Pneumococcal Conjugate Vaccine 13*/
    val pneuConjuVaccine13List = List(KpiConstants.pneuConjuVaccine13Val)
    val pneuConjuVaccine13CodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal,KpiConstants.snomedctCodeVal)
    val joinedForpneuConjuVaccineDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,pneuConjuVaccine13List,pneuConjuVaccine13CodeSystem)
                                                     .select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val memDfJoinpneuConjuVaccineDf = memDf.as("df1").join(joinedForpneuConjuVaccineDf.as("df2"),memDf.col(KpiConstants.memberskColName) === joinedForpneuConjuVaccineDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                                              .filter(datediff(joinedForpneuConjuVaccineDf.col(KpiConstants.startDateColName),memDf.col(KpiConstants.sixtyDobColName)).>=(0)).groupBy(memDf.col(KpiConstants.memberskColName))
                                                              .agg(min(joinedForpneuConjuVaccineDf.col(KpiConstants.startDateColName)).alias(KpiConstants.startDateColName))

    //memDfJoinpneuConjuVaccineDf.printSchema()


    /*Pneumococcal Polysaccharide Vaccine 23*/
    val pneuPolyVaccine23List = List(KpiConstants.pneuPolyVaccine23Val)
    val pneuPolyVaccine23CodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinedForpneuPolyVaccDf = UtilFunctions.factClaimRefHedisJoinFunction(spark,factClaimDf,refHedisDf,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.emptyMesureId,pneuPolyVaccine23List,pneuPolyVaccine23CodeSystem).select(KpiConstants.memberskColName,KpiConstants.startDateColName)

    val memDfJoinpneuPolyVaccDf = memDf.as("df1").join(joinedForpneuPolyVaccDf.as("df2"),memDf.col(KpiConstants.memberskColName) === joinedForpneuPolyVaccDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType)
                                                        .filter(datediff(joinedForpneuPolyVaccDf.col(KpiConstants.startDateColName),memDf.col(KpiConstants.sixtyDobColName)).>=(0)).groupBy(memDf.col(KpiConstants.memberskColName))
                                                        .agg(min(joinedForpneuPolyVaccDf.col(KpiConstants.startDateColName)).alias(KpiConstants.startDateColName))

    val numDf = memDfJoinpneuConjuVaccineDf.as("df1").join(memDfJoinpneuPolyVaccDf.as("df2"),memDfJoinpneuConjuVaccineDf.col(KpiConstants.memberskColName) === memDfJoinpneuPolyVaccDf.col(KpiConstants.memberskColName),KpiConstants.innerJoinType).filter(datediff($"df1.start_date",$"df2.start_date").===(365)).select($"df1.member_sk")
    val numeratorDf = numDf.intersect(dinominatorMemSkDf)
    /*Numerator calculation ends*/
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = pneuConjuVaccine13List ::: pneuPolyVaccine23List
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
