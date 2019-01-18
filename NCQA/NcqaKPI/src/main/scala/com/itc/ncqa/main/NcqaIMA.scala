package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object NcqaIMA {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIMA")
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
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.imaMeasureTitle)

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name))) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*doing age filter 15 months old during the measurement year */
    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"
    val years13ColAddedDf = commonFilterDf.withColumn(KpiConstants.thirteenDobColName, add_months(commonFilterDf.col(KpiConstants.dobColName), KpiConstants.months156))
      .withColumn(KpiConstants.twelveDobColName, add_months(commonFilterDf.col(KpiConstants.dobColName), KpiConstants.months144))
      .withColumn(KpiConstants.elevenDobColName, add_months(commonFilterDf.col(KpiConstants.dobColName), KpiConstants.months132))
      .withColumn(KpiConstants.tenDobColName, add_months(commonFilterDf.col(KpiConstants.dobColName), KpiConstants.months120))
      .withColumn(KpiConstants.nineDobColName, add_months(commonFilterDf.col(KpiConstants.dobColName), KpiConstants.months108))
    val ageFilterDf = years13ColAddedDf.filter((years13ColAddedDf.col(KpiConstants.thirteenDobColName).>=(ageStartDate)) && (years13ColAddedDf.col(KpiConstants.thirteenDobColName).<=(ageEndDate)))

    /*Continuous Enrollment Checking*/
    val contEnrollDf = ageFilterDf.filter((ageFilterDf.col(KpiConstants.memStartDateColName).<=(ageFilterDf.col(KpiConstants.twelveDobColName))) &&
      (ageFilterDf.col(KpiConstants.memEndDateColName).>=(ageFilterDf.col(KpiConstants.thirteenDobColName))))

    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = contEnrollDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    val filterdForDinoExclDf = contEnrollDf.select(KpiConstants.memberskColName, KpiConstants.thirteenDobColName)
    /*Dinominator Exclusion1(Anaphylactic Reaction Due To Vaccination)*/

    /* val ardvVal = "Anaphylactic Reaction Due To Vaccination"
     val ardtsVal = "Anaphylactic Reaction Due To Serum" */
    val imaDinoExclValSet1 = List(KpiConstants.ardvVal)
    val joinForDinoExcl1 = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet1, primaryDiagnosisCodeSystem)
    val onOrBef13yeardf = joinForDinoExcl1.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
      .select(s"df1.${KpiConstants.memberskColName}")

    /*Dinominator Exclusion2(Anaphylactic Reaction Due To Serum Value Set)*/

    val filterOct1st2011 = "2011-10-01"

    val imaDinoExclValSet2 = List(KpiConstants.ardtsVal)
    val joinForDinoExcl2 = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet2, primaryDiagnosisCodeSystem)
    val onOrBefOct1st2011f = joinForDinoExcl2.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.startDateColName}".<=(filterOct1st2011))
      .select(s"df1.${KpiConstants.memberskColName}")

    /*Dinominator Exclusion3 (Encephalopathy Due To Vaccination Value Set) ) */

    /*  val encephalopathyVal = "Encephalopathy Due To Vaccination"
      val vaccineAdverseVal = "Vaccine Causing Adverse Effect" */

    val imaDinoExclValSet3a = List(KpiConstants.encephalopathyVal)
    val joinForDinoExcl3a = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3a, primaryDiagnosisCodeSystem)
    val mesrForDinoExcl3a = joinForDinoExcl3a.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                              .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
                                              .select(s"df1.${KpiConstants.memberskColName}")

    /*Dinominator Exclusion3and (Vaccine Causing Adverse Effect Value Set) */

    val imaDinoExclValSet3b = List(KpiConstants.vaccineAdverseVal)
    val joinForDinoExcl3b = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3b, primaryDiagnosisCodeSystem)
    val mesrForDinoExcl3b = joinForDinoExcl3b.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                             .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
                                              .select(s"df1.${KpiConstants.memberskColName}")
    /* Encephalopathy Due To Vaccination Value Set with Vaccine Causing Adverse Effect Value Set */

    val imaExlc3Df = mesrForDinoExcl3a.intersect(mesrForDinoExcl3b)



    /* Union of all the Dinominator Exclusions */

    val dinoExcl2Df = onOrBef13yeardf.union(onOrBefOct1st2011f).union(imaExlc3Df).select(KpiConstants.memberskColName)

    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunction(spark, factClaimDf, refHedisDf).select(KpiConstants.memberskColName).distinct()
    val dinominatorExclDf = hospiceDf.union(dinoExcl2Df)


    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
    dinominatorAfterExclusionDf.printSchema()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    /*Numerator1 Calculation (IMAMEN screening or monitoring test)*/

    val imaMenValueSet = List(KpiConstants.meningococcalVal)
    val imaMenCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForImamenScreenDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaMenValueSet, imaMenCodeSystem)
    val imaMenAgeFilterDf = joinForImamenScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                 .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.elevenDobColName}") )
                                                 .select(KpiConstants.memberskColName)

    /*Numerator1 Calculation (IMATD screening or monitoring test)*/

    val imaTdapValueSet = List(KpiConstants.tdapVaccineVal)
    val imaTdapCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForImaTdapScreenDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaTdapValueSet, imaTdapCodeSystem)
    val imaTdapAgeFilterDf = joinForImaTdapScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                   .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.tenDobColName}") )
                                                   .select(KpiConstants.memberskColName)

    /*Numerator1 Calculation (IMAHPV screening or monitoring test)*/
    val imaHpvValueSet = List(KpiConstants.hpvVal)
    val imaHpvCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForimaHpvScreenDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaHpvValueSet, imaHpvCodeSystem)
    val imaHpvAgeFilterDf = joinForimaHpvScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.nineDobColName}") )
      .select("df1.*")
    val imaHpvAtleaset3Df =  imaHpvAgeFilterDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val)).select(KpiConstants.memberskColName)
    /* 	There must be at least 146 days between the first and second dose  ---- Pending */

    /*Numerator1 Calculation (Combination 1 (Meningococcal, Tdap))*/

    val imaCmb1Df = imaMenAgeFilterDf.intersect(imaTdapAgeFilterDf).select(KpiConstants.memberskColName)

    /*Numerator1 Calculation (Combination 2 (Meningococcal, Tdap, HPV))*/

    val imaCmb2Df = imaMenAgeFilterDf.intersect(imaTdapAgeFilterDf).intersect(imaHpvAtleaset3Df).select(KpiConstants.memberskColName)

    /*find out the member_sk based on the measure id*/

    val imaNumeratorDf =  measureId match {

      case KpiConstants.imamenMeasureId => imaMenAgeFilterDf
      case KpiConstants.imatdMeasureId => imaTdapAgeFilterDf
      case KpiConstants.imahpvMeasureId => imaHpvAtleaset3Df
      case KpiConstants.imacmb1MeasureId => imaCmb1Df
      case KpiConstants.imacmb2MeasureId => imaCmb2Df

    }

    val numeratorDf =  imaNumeratorDf.intersect(dinominatorAfterExclusionDf)
    numeratorDf.show()

    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/

    /* Numerator valueset based on MeasureID */
    val numeratorVal =  measureId match {
      case KpiConstants.imamenMeasureId => imaMenValueSet
      case KpiConstants.imatdMeasureId => imaTdapValueSet
      case KpiConstants.imahpvMeasureId => imaHpvValueSet
      case KpiConstants.imacmb1MeasureId => imaMenValueSet:::imaTdapValueSet
      case KpiConstants.imacmb2MeasureId => imaMenValueSet:::imaTdapValueSet:::imaHpvValueSet
    }

    val numeratorValueSet = numeratorVal
    val dinominatorExclValueSet = imaDinoExclValSet1:::imaDinoExclValSet3a:::imaDinoExclValSet3b
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
