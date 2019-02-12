package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import com.sun.org.apache.xml.internal.security.transforms.implementations.TransformXSLT
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, countDistinct, datediff, rank,first}

object NcqaIMAClinical {

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("NcqaIMAClinical")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val dimPatientDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimPatientTblName, data_source)
    val dimProductDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProductTblName, data_source)
    val factPatMemDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factpatmemTblName, data_source)
    val factImmunDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factImmunTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val factencDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName, KpiConstants.factencTblName, data_source)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    //</editor-fold>

    //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

    /*Initial join function call for prepare the data fro common filter*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunctionForClinical(spark,dimMemberDf,dimPatientDf,factImmunDf,factMembershipDf,factPatMemDf,dimProductDf,dimLocationDf,refLobDf,dimFacilityDf,lob_name,KpiConstants.imaMeasureTitle)

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
    dinominatorDf.printSchema()
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.patientSkColname)
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    val filterdForDinoExclDf = contEnrollDf.select(KpiConstants.patientSkColname, KpiConstants.thirteenDobColName)
    val factEncDxDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName, KpiConstants.factencdxTblName,data_source)
    /*Dinominator Exclusion1(Anaphylactic Reaction Due To Vaccination)*/

    val valList = List(KpiConstants.meningococcalVal,KpiConstants.tdapVaccineVal,KpiConstants.hpvVal)
    val joinForDinoExclDf = UtilFunctions.factImmEncRefHedisJoinFunction(spark,factImmunDf, factEncDxDf,refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, valList, primaryDiagnosisCodeSystem)
                                        .select(s"${KpiConstants.patientSkColname}")
    val membersDf = contEnrollDf.select(KpiConstants.patientSkColname).except(joinForDinoExclDf)


    val imaDinoExclValSet1 = List(KpiConstants.ardvVal)
    val joinForDinoExcl1 = UtilFunctions.factImmEncRefHedisJoinFunction(spark,factImmunDf, factEncDxDf,refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet1, primaryDiagnosisCodeSystem)
                                        .select(s"${KpiConstants.patientSkColname}", s"${KpiConstants.immuneDateColName}")
    val onOrBef13yeardf = joinForDinoExcl1.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                          .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
                                          .select(s"df1.${KpiConstants.patientSkColname}")

    /*Dinominator Exclusion2(Anaphylactic Reaction Due To Serum Value Set)*/
    val filterOct1st2011 = "2011-10-01"
    val imaDinoExclValSet2 = List(KpiConstants.ardtsVal)
    val joinForDinoExcl2 = UtilFunctions.factImmEncRefHedisJoinFunction(spark,factImmunDf, factEncDxDf,refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet2, primaryDiagnosisCodeSystem)
    val onOrBefOct1st2011f = joinForDinoExcl2.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                             .filter($"df1.${KpiConstants.immuneDateColName}".<=(filterOct1st2011))
                                             .select(s"df1.${KpiConstants.patientSkColname}")

    /*Dinominator Exclusion3 (Encephalopathy Due To Vaccination Value Set) ) */
    val imaDinoExclValSet3a = List(KpiConstants.encephalopathyVal)
    val joinForDinoExcl3a = UtilFunctions.factImmEncRefHedisJoinFunction(spark,factImmunDf, factEncDxDf,refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3a, primaryDiagnosisCodeSystem)
    val mesrForDinoExcl3a = joinForDinoExcl3a.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                             .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
                                             .select(s"df1.${KpiConstants.patientSkColname}")

    /*Dinominator Exclusion3and (Vaccine Causing Adverse Effect Value Set) */
    val imaDinoExclValSet3b = List(KpiConstants.vaccineAdverseVal)
    val joinForDinoExcl3b = UtilFunctions.factImmEncRefHedisJoinFunction(spark,factImmunDf, factEncDxDf,refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3b, primaryDiagnosisCodeSystem)
    val mesrForDinoExcl3b = joinForDinoExcl3b.as("df1").join(filterdForDinoExclDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                             .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}"))
                                             .select(s"df1.${KpiConstants.patientSkColname}")

    /* Encephalopathy Due To Vaccination Value Set with Vaccine Causing Adverse Effect Value Set */
    val imaExlc3Df = mesrForDinoExcl3a.intersect(mesrForDinoExcl3b)

    /* Union of all the Dinominator Exclusions */
    val dinoExcl2Df = onOrBef13yeardf.union(onOrBefOct1st2011f).union(imaExlc3Df)
    val optionalExclDf = membersDf.intersect(dinoExcl2Df)
    /*find out the hospice members*/
    val hospiceDf = UtilFunctions.hospiceFunctionForClinical(spark,factencDf,factEncDxDf,refHedisDf)
    val dinominatorExclDf = hospiceDf.union(optionalExclDf)


    /*Dinominator After Exclusion*/
    val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    dinominatorAfterExclusionDf.printSchema()

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="IMAMEN">

    /*Numerator1 Calculation (IMAMEN screening or monitoring test)*/
    val imaMenValueSet = List(KpiConstants.meningococcalVal)
    val imaMenCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForImamenScreenDf = UtilFunctions.factImmRefHedisJoinFunction(spark,factImmunDf, refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaMenValueSet, imaMenCodeSystem)
    val imaMenAgeFilterDf = joinForImamenScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                                 .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.immuneDateColName}".>=($"df2.${KpiConstants.elevenDobColName}") )
                                                 .select(s"df1.${KpiConstants.patientSkColname}")
    //</editor-fold>

    //<editor-fold desc="IMATDAP">

    /*Numerator2 Calculation (IMATD screening or monitoring test)*/
    val imaTdapValueSet = List(KpiConstants.tdapVaccineVal)
    val imaTdapCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForImaTdapScreenDf = UtilFunctions.factImmRefHedisJoinFunction(spark,factImmunDf, refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaTdapValueSet, imaTdapCodeSystem)
    val imaTdapAgeFilterDf = joinForImaTdapScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                                   .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.immuneDateColName}".>=($"df2.${KpiConstants.tenDobColName}") )
                                                   .select(s"df1.${KpiConstants.patientSkColname}")
    //</editor-fold>

    //<editor-fold desc="IMAHPV">

    /*Numerator3 Calculation (IMAHPV screening or monitoring test)*/
    val imaHpvValueSet = List(KpiConstants.hpvVal)
    val imaHpvCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val joinForimaHpvScreenDf = UtilFunctions.factImmRefHedisJoinFunction(spark,factImmunDf, refHedisDf, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaHpvValueSet, imaHpvCodeSystem)
    val imaHpvAgeFilterDf = joinForimaHpvScreenDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}",KpiConstants.innerJoinType)
                                                 .filter($"df1.${KpiConstants.immuneDateColName}".<=($"df2.${KpiConstants.thirteenDobColName}") && $"df1.${KpiConstants.immuneDateColName}".>=($"df2.${KpiConstants.nineDobColName}") )
                                                 .select("df1.*")



    /*At least 2 HPV test between 9 and 13th birthday*/
    val imaHpvAtleast2Df = imaHpvAgeFilterDf.groupBy(KpiConstants.patientSkColname).agg(countDistinct(KpiConstants.immuneDateColName).alias(KpiConstants.countColName))
                                            .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                            .select(KpiConstants.patientSkColname)
    /*ima Hpv with start date*/
    val imaHpvStDateDf = imaHpvAgeFilterDf.as("df1").join(imaHpvAtleast2Df.as("df2"),$"df1.${KpiConstants.patientSkColname}" === $"df2.${KpiConstants.patientSkColname}", KpiConstants.innerJoinType)
                                          .select(s"df1.${KpiConstants.patientSkColname}",s"df1.${KpiConstants.immuneDateColName}")
    imaHpvStDateDf.printSchema()

    /*window creation for finding out the first and second date for each member_sk*/
    val windowVal = Window.partitionBy(s"${KpiConstants.patientSkColname}").orderBy(s"${KpiConstants.immuneDateColName}")

    /*imHpv with at least 2 visit (member_sk and first 2 visit dates)*/
    val imaHpv1Df = imaHpvStDateDf.withColumn(KpiConstants.rankColName, rank().over(windowVal)).withColumn(KpiConstants.immuneDate2ColName, first(KpiConstants.immuneDateColName).over(windowVal))
                                     .filter(($"${KpiConstants.rankColName}".<=(KpiConstants.count2Val)) && (datediff($"${KpiConstants.immuneDateColName}",$"${KpiConstants.immuneDate2ColName}").>=(KpiConstants.days146)))
                                     .select(s"${KpiConstants.patientSkColname}")



    /*ImaHPV numerator first part (at least 3 HPV test between 9 and 13th birthday)*/
    val imaHpv2Df =  imaHpvAgeFilterDf.groupBy(KpiConstants.patientSkColname).agg(countDistinct(KpiConstants.immuneDateColName).alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val))
                                              .select(KpiConstants.patientSkColname)



    val imaHpvDf = imaHpv1Df.union(imaHpv2Df)
    //</editor-fold>

    /*Numerator4 Calculation (Combination 1 (Meningococcal, Tdap))*/
    val imaCmb1Df = imaMenAgeFilterDf.intersect(imaTdapAgeFilterDf)

    /*Numerator5 Calculation (Combination 2 (Meningococcal, Tdap, HPV))*/
    val imaCmb2Df = imaMenAgeFilterDf.intersect(imaTdapAgeFilterDf).intersect(imaHpvDf)
    imaCmb2Df.printSchema()

    /*numeratorDf and the numerator vcalueset based on the measure id*/
    var imaNumeratorDf = spark.emptyDataFrame
    var numeratorVal = KpiConstants.emptyList
    measureId match {

      case KpiConstants.imamenMeasureId  => imaNumeratorDf = imaMenAgeFilterDf
        numeratorVal = imaMenValueSet

      case KpiConstants.imatdMeasureId   => imaNumeratorDf = imaTdapAgeFilterDf
        numeratorVal = imaTdapValueSet

      case KpiConstants.imahpvMeasureId  => imaNumeratorDf = imaHpvDf
        numeratorVal = imaHpvValueSet

      case KpiConstants.imacmb1MeasureId => imaNumeratorDf = imaCmb1Df
        numeratorVal = imaMenValueSet:::imaTdapValueSet

      case KpiConstants.imacmb2MeasureId => imaNumeratorDf = imaCmb2Df
        numeratorVal = imaMenValueSet:::imaTdapValueSet:::imaHpvValueSet

    }

    val numeratorDf =  imaNumeratorDf.intersect(dinominatorAfterExclusionDf)
    //numeratorDf.show()
    //</editor-fold>


  }
}
