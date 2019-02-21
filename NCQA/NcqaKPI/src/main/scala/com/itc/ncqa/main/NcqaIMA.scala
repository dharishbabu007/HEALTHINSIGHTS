package com.itc.ncqa.main

import java.sql.Date

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import com.itc.ncqa.Functions.SparkObject._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.mutable

case class Member(member_sk:String, service_date:Date)
case class GroupMember(member_sk:String, dateList:List[Long])

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

   /* /*creating spark session object*/
    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAIMA")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()*/

    import spark.implicits._

    //</editor-fold>

    //<editor-fold desc="Loading of Required Tables">

    /*Loading dim_member,fact_claims,fact_membership tables */
    val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
    val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
    val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
    val factMemAttrDf = DataLoadFunctions.dataLoadFromTargetModel(spark,KpiConstants.dbName, KpiConstants.factMemAttrTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
   // val dimQualityMsrDf = DataLoadFunctions.dimqualityMeasureLoadFunction(spark,KpiConstants.imaMeasureTitle)
   // val dimQualityPgmDf = DataLoadFunctions.dimqualityProgramLoadFunction(spark, KpiConstants.hedisPgmname)
    val dimProductPlanDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName,KpiConstants.dimProductTblName,data_source)
    /*loading ref_hedis table*/
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem
    factClaimDf.cache()
    refHedisDf.cache()
    //</editor-fold>

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Initial join function">

    val argmapForInitJoin = mutable.Map(KpiConstants.dimMemberTblName -> dimMemberDf, KpiConstants.factMembershipTblName -> factMembershipDf,
                                        KpiConstants.dimProductTblName -> dimProductPlanDf, KpiConstants.refLobTblName -> refLobDf,
                                        KpiConstants.factMemAttrTblName -> factMemAttrDf, KpiConstants.dimDateTblName -> dimDateDf)
    val initialJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForInitJoin)

    //initialJoinedDf.printSchema()
    //initialJoinedDf.show(50)
    //</editor-fold>

    //<editor-fold desc="Age filter">

    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageFilterDf = initialJoinedDf.filter((add_months($"${KpiConstants.dobColName}",KpiConstants.months156).>=(ageStartDate)) && (add_months($"${KpiConstants.dobColName}",KpiConstants.months156).<=(ageEndDate)))
    //ageFilterDf.show(50)
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberskColName, KpiConstants.dobColName, KpiConstants.benefitMedicalColname, KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,KpiConstants.lobColName)
    val argMap = mutable.Map(KpiConstants.ageStartKeyName -> "12", KpiConstants.ageEndKeyName -> "13", KpiConstants.ageAnchorKeyName -> "13",
                             KpiConstants.lobNameKeyName -> lob_name, KpiConstants.benefitKeyName -> KpiConstants.benefitMedicalColname)

    val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.ageformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                  .select("df1.*")
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

    val argmapForHospice = mutable.Map(KpiConstants.eligibleDfName -> contEnrollDf.select(KpiConstants.memberskColName), KpiConstants.factClaimTblName -> factClaimDf,
                                       KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf)
    val hospiceDf = UtilFunctions.hospiceExclusionFunction(spark, argmapForHospice,year)
    //</editor-fold>

    /*eligble population for IMA measure*/
    val eligibleDf = contEnrollDf.as("df1").join(hospiceDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                                  .filter($"df2.${KpiConstants.memberskColName}".===(null))
                                                  .select(s"df1.${KpiConstants.memberskColName}", KpiConstants.productplanSkColName, KpiConstants.qualityMsrSkColName,KpiConstants.dobColName)

    eligibleDf.cache()
    eligibleDf.printSchema()

    val filterdEligibleDf = eligibleDf.select(KpiConstants.memberskColName, KpiConstants.dobColName)
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val dinominatorDf = eligibleDf
    val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName)
    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val dfMapForCalculation = mutable.Map(KpiConstants.eligibleDfName -> filterdEligibleDf, KpiConstants.factClaimTblName -> factClaimDf ,
                                          KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf)
    /*Dinominator Exclusion1(Anaphylactic Reaction Due To Vaccination)*/

    /*Find memebers who has any of the 3 vaccines*/
    val valList = List(KpiConstants.meningococcalVal,KpiConstants.hpvVal,KpiConstants.tdapVaccineVal)
    val codeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForDinoExcl1Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, valList, codeSystem)
                                            .select(KpiConstants.memberskColName)



    /*ind memebers who has  Tdap vaccines*/
    val tdapvalList = List(KpiConstants.tdapVaccineVal)
    val joinedForTdap = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, tdapvalList, codeSystem)
                                     .select(KpiConstants.memberskColName)

    /*Find out the members who has not the vaccines based on the measure id*/
    val membersDf = measureId match {


      case KpiConstants.imatdMeasureId => eligibleDf.select(KpiConstants.memberskColName).except(joinedForTdap)

      case KpiConstants.imamenMeasureId => eligibleDf.select(KpiConstants.memberskColName).except(joinedForDinoExcl1Df)

      case KpiConstants.imahpvMeasureId => eligibleDf.select(KpiConstants.memberskColName).except(joinedForDinoExcl1Df)

      case KpiConstants.imacmb1MeasureId => eligibleDf.select(KpiConstants.memberskColName).except(joinedForDinoExcl1Df)

      case KpiConstants.imacmb2MeasureId => eligibleDf.select(KpiConstants.memberskColName).except(joinedForDinoExcl1Df)
    }


    //<editor-fold desc="ARDV on or before 13th birth day">

    val imaDinoExclValSet1 = List(KpiConstants.ardvVal)
    val ardvBef13yeardf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet1, primaryDiagnosisCodeSystem)
                                        .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                        .select(s"${KpiConstants.memberskColName}")
    //</editor-fold>

    //<editor-fold desc="Anaphylactic Reaction Due To Serum Value Set">

    val filterOct1st2011 = "2011-10-01"
    val imaDinoExclValSet2 = List(KpiConstants.ardtsVal)
    val ardtsrBefOct1st2011Df = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet2, primaryDiagnosisCodeSystem)
                                             .filter($"${KpiConstants.serviceDateColName}".<=(filterOct1st2011))
                                             .select(s"${KpiConstants.memberskColName}")
    //</editor-fold>

    //<editor-fold desc="Encephalopathy Due To Vaccination Value Set">

    val imaDinoExclValSet3a = List(KpiConstants.encephalopathyVal)
    val edvBefore13YearDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3a, primaryDiagnosisCodeSystem)
                                         .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                         .select(KpiConstants.memberskColName)


    /*Dinominator Exclusion3and (Vaccine Causing Adverse Effect Value Set) */
    val imaDinoExclValSet3b = List(KpiConstants.vaccineAdverseVal)
    val vcabefore13YearDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForCalculation, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaDinoExclValSet3b, primaryDiagnosisCodeSystem)
                                         .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                         .select(s"${KpiConstants.memberskColName}")

    /* Encephalopathy Due To Vaccination Value Set with Vaccine Causing Adverse Effect Value Set */
    val imaExlc3Df = edvBefore13YearDf.intersect(vcabefore13YearDf)
    //</editor-fold>


    /*Optional exclusion based on the Measure id*/
    val optionalDinoxclDf = measureId match {

      case KpiConstants.imamenMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df)

      case KpiConstants.imatdMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df)

      case KpiConstants.imahpvMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df)

      case KpiConstants.imacmb1MeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df)

      case KpiConstants.imacmb2MeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df)
    }

    val optionalExclDf = membersDf.intersect(optionalDinoxclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)

    //<editor-fold desc="IMAMEN">

    /*Numerator1 Calculation (IMAMEN screening or monitoring test)*/
    val imaMenValueSet = List(KpiConstants.meningococcalVal)
    val imaMenCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imamenNumDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaMenValueSet, imaMenCodeSystem)
                                             .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}", KpiConstants.months132))) && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}", KpiConstants.months156)))
                                                      && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList)))
                                             .select(s"${KpiConstants.memberskColName}")

    //</editor-fold>

    //<editor-fold desc="IMATDAP">

    /*Numerator2 Calculation (IMATD screening or monitoring test)*/
    val imaTdapValueSet = List(KpiConstants.tdapVaccineVal)
    val imaTdapCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imaTdapNumDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaTdapValueSet, imaTdapCodeSystem)
                                              .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}",KpiConstants.months120))) && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                                       && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList)))
                                              .select(s"${KpiConstants.memberskColName}")

    //</editor-fold>

    //<editor-fold desc="IMAHPV">

    /*Numerator3 Calculation (IMAHPV screening or monitoring test)*/
    val imaHpvValueSet = List(KpiConstants.hpvVal)
    val imaHpvCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imaHpvAgeFilterDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaHpvValueSet, imaHpvCodeSystem)
                                         .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}", KpiConstants.months108))) && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}", KpiConstants.months156)))
                                                  && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList)))
                                         .select(s"${KpiConstants.memberskColName}", s"${KpiConstants.serviceDateColName}")




    /*HPV First Condition(atleast 2 date of service with 146 days gap)*/
    val imaHpv1Df = imaHpvAgeFilterDf.groupBy(KpiConstants.memberskColName).agg(count(when(datediff(max($"${KpiConstants.serviceDateColName}"), min($"${KpiConstants.serviceDateColName}")).>=(146), 1)).alias(KpiConstants.countColName))
                                            .filter($"${KpiConstants.countColName}".>=(KpiConstants.count1Val))
                                            .select(KpiConstants.memberskColName)



    val imaHpv2InDf = imaHpvAgeFilterDf.as("df1").join(imaHpv1Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                       .filter($"df2.${KpiConstants.memberskColName}".isNull)
                                       .select("df1.*")

    val inDs = imaHpv2InDf.as[Member]

    val groupedDs = inDs.groupByKey(inDs => (inDs.member_sk))
                        .mapGroups((k,itr) => (k,itr.map(f=> f.service_date.getTime).toArray.sorted))


    val imaHpv2Df = groupedDs.map(f=> UtilFunctions.getMembers(f._1,f._2))
                            .filter(f=> f._2.equals("Y")).select("_1").toDF("member_sk")


    val imaHpvDf = imaHpv1Df.union(imaHpv2Df)
    //</editor-fold>

    /*Numerator4 Calculation (Combination 1 (Meningococcal, Tdap))*/
    val imaCmb1Df = imamenNumDf.intersect(imaTdapNumDf)

    /*Numerator5 Calculation (Combination 2 (Meningococcal, Tdap, HPV))*/
    val imaCmb2Df = imamenNumDf.intersect(imaTdapNumDf).intersect(imaHpvDf)


    /*numeratorDf and the numerator vcalueset based on the measure id*/
    var imaNumeratorDf = spark.emptyDataFrame
    var numeratorVal = KpiConstants.emptyList
    measureId match {

      case KpiConstants.imamenMeasureId  => imaNumeratorDf = imamenNumDf
                                            numeratorVal = imaMenValueSet

      case KpiConstants.imatdMeasureId   => imaNumeratorDf = imaTdapNumDf
                                            numeratorVal = imaTdapValueSet

      case KpiConstants.imahpvMeasureId  => imaNumeratorDf = imaHpvDf
                                            numeratorVal = imaHpvValueSet

      case KpiConstants.imacmb1MeasureId => imaNumeratorDf = imaCmb1Df
                                            numeratorVal = imaMenValueSet:::imaTdapValueSet

      case KpiConstants.imacmb2MeasureId => imaNumeratorDf = imaCmb2Df
                                            numeratorVal = imaMenValueSet:::imaTdapValueSet:::imaHpvValueSet

    }

    val numeratorDf =  imaNumeratorDf
    //numeratorDf.show()
    //</editor-fold>


/*
    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
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
*/

    spark.sparkContext.stop()
  }

}
