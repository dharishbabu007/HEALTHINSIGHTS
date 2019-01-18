package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object  NcqaCIS {

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

    val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACIS")
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
    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refHedisTblName)
    //</editor-fold>

    //<editor-fold desc="Initial Join, Allowable Gap,Age filter and Continuous enrollment">

    /*Join dimMember,factclaim,factmembership,reflob,dimfacility,dimlocation.*/
    val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.cisMeasureTitle)

    /*Allowable gap filter Calculation */
    var lookUpDf = spark.emptyDataFrame
    if(KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view45Days)
    }
    else{

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark,KpiConstants.view60Days)
    }

    /*common filter checking*/
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"),initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName),KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*second dob column added to the df for agefilter*/
    val dob1And2AddedDf = commonFilterDf.withColumn(KpiConstants.secondDobColName, add_months($"${KpiConstants.dobColName}",KpiConstants.months24))
                                        .withColumn(KpiConstants.firstDobColName, add_months($"${KpiConstants.dobColName}",KpiConstants.months12))

    val endDate = year + "-12-31"
    val startDate = year + "-01-01"

    /*Age filter( members whoose second birthday in the measurement year.)*/
    val ageFilterDf = dob1And2AddedDf.filter(($"${KpiConstants.secondDobColName}".<=(endDate)) && ($"${KpiConstants.secondDobColName}".>=(startDate)))
    //ageFilterDf.printSchema()

    /*Continuous Enrollment filter*/
    val continuousEnrollDf = ageFilterDf.filter(($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.firstDobColName}")) && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.secondDobColName}")))
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = continuousEnrollDf
    val dinominatorForKpiCalDf = dinominatorDf.select(s"${KpiConstants.memberskColName}")
    //</editor-fold>

    //<editor-fold desc="Dinominator Exclusion Calculation">

    val primaryDiagCodeSystem = List(KpiConstants.icdCodeVal)
    val filterdDfForDinoExclDf = continuousEnrollDf.select(KpiConstants.memberskColName,KpiConstants.secondDobColName)

    //<editor-fold desc="Hospice">

    val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()
    //</editor-fold>

    //<editor-fold desc="Any particular vaccine">

    val anaReactVacValList = List(KpiConstants.anaReactVacVal)
    val joinedForanaReactVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, anaReactVacValList, primaryDiagCodeSystem)
                                              .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForanaReactVacDf = filterdDfForDinoExclDf.as("df1").join(joinedForanaReactVacDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                      .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                      .select(s"df1.${KpiConstants.memberskColName}")
    //</editor-fold>

    //<editor-fold desc="Dtap">

    /*Encephalopathy Due To Vaccination value list*/
    val encephalVacValList = List(KpiConstants.encephalopathyVal)
    val joinedForencephalVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, encephalVacValList, primaryDiagCodeSystem)
                                              .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForencephalVacDf = filterdDfForDinoExclDf.as("df1").join(joinedForencephalVacDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                      .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                      .select(s"df1.${KpiConstants.memberskColName}")

    /*Vaccine Causing Adverse Effect valuelist*/
    val vacCauseAdvEffValList = List(KpiConstants.vacCauseAdvEffVal)
    val joinedForvacCauseAdvEffValDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, vacCauseAdvEffValList, primaryDiagCodeSystem)
                                                    .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForvacCauseAdvEffValDf = filterdDfForDinoExclDf.as("df1").join(joinedForvacCauseAdvEffValDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                            .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                            .select(s"df1.${KpiConstants.memberskColName}")

    val dtapDf = measrForencephalVacDf.intersect(measrForvacCauseAdvEffValDf)
    //</editor-fold>

    //<editor-fold desc="MMR,VZV,influenza">

    /*Immunodeficiency*/
    val disorderImmSysValList = List(KpiConstants.disordersoftheImmuneSystemVal)
    val joinedFordisorderImmSysDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, disorderImmSysValList, primaryDiagCodeSystem)
                                                 .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFordisorderImmSysDf = filterdDfForDinoExclDf.as("df1").join(joinedFordisorderImmSysDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                         .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                         .select(s"df1.${KpiConstants.memberskColName}")

    /*HIV*/
    val hivValList = List(KpiConstants.hivVal, KpiConstants.hivType2Val)
    val joinedForhivDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, hivValList, primaryDiagCodeSystem)
                                      .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForhivDf = filterdDfForDinoExclDf.as("df1").join(joinedForhivDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                              .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                              .select(s"df1.${KpiConstants.memberskColName}")

    /*Malignant Neoplasm of Lymphatic Tissue*/
    val mallNeoLymTisValList = List(KpiConstants.mallNeoLymTisVal)
    val joinedFormallNeoLymTisDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, mallNeoLymTisValList, primaryDiagCodeSystem)
                                                .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormallNeoLymTisDf = filterdDfForDinoExclDf.as("df1").join(joinedFormallNeoLymTisDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                        .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                        .select(s"df1.${KpiConstants.memberskColName}")

    /*pending*/
    /*Anaphylactic reaction to neomycin*/

    val mmrvzvInfluenzaDf = measrFordisorderImmSysDf.union(measrForhivDf).union(measrFormallNeoLymTisDf)
    //</editor-fold>

    //<editor-fold desc="Rotavirus">

    /*Severe Combined Immunodeficiency valueset*/
    val severeCombImmunValList = List(KpiConstants.severeCombImmunVal)
    val joinedForsevCombImmuDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, severeCombImmunValList, primaryDiagCodeSystem)
                                            .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForsevCombImmuDf = filterdDfForDinoExclDf.as("df1").join(joinedForsevCombImmuDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                    .select(s"df1.${KpiConstants.memberskColName}")

    /*Intussusception value set*/
    val intussusceptionValList = List(KpiConstants.intussusceptionVal)
    val joinedForintussusceptionDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType,KpiConstants.cisMeasureId, intussusceptionValList, primaryDiagCodeSystem)
                                                  .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForintussusceptionDf = filterdDfForDinoExclDf.as("df1").join(joinedForintussusceptionDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                          .filter($"df2.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}"))
                                                          .select(s"df1.${KpiConstants.memberskColName}")


    val rotavirusDf = measrForsevCombImmuDf.union(measrForintussusceptionDf)

    //</editor-fold>

    /*IPV and Hepatitis B are pending*/

    /*dinominator Exclusion union*/
    val dinominatorExclDf = hospiceDf.union(measrForanaReactVacDf).union(dtapDf).union(mmrvzvInfluenzaDf).union(rotavirusDf)
    val dinominatorAfterExclDf = dinominatorForKpiCalDf.except(dinominatorExclDf)
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation and Numerator Reason set calculation based On measure id">

    val filterdForNumCalDf = continuousEnrollDf.select(KpiConstants.memberskColName,KpiConstants.secondDobColName, KpiConstants.dobColName, KpiConstants.firstDobColName)

    //<editor-fold desc="Atleast 4 Dtap Numerator">

    val dtapVacAdmValList = List(KpiConstants.dtapVacAdmVal)
    val dtapVacAdmCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedFordtapVacAdmDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, dtapVacAdmValList, dtapVacAdmCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the proceedures that has done after 42 days to 2nd dob*/
    val measrFordtapVacAdmDf = joinedFordtapVacAdmDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                    .select("df1.*")

    val dtapNumDf = measrFordtapVacAdmDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                        .filter($"${KpiConstants.countColName}".>=(KpiConstants.count4Val))
                                        .select(KpiConstants.memberskColName)

    //</editor-fold>

    //<editor-fold desc="Atleast 3 IPV vaccination">

    val ipvVaccineValList = List(KpiConstants.ipvVaccineVal)
    val ipvVaccineCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForIpvVaccineDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, ipvVaccineValList, ipvVaccineCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the ipv proceedures that has done after 42 days to 2nd dob*/
    val measrForIpvVaccineDf = joinedForIpvVaccineDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                    .select("df1.*")

    /*atleast 3 ipv proceedure during the dob+42 to 2nd dob period*/
    val ipvNumDf = measrForIpvVaccineDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                       .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val))
                                       .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="MMR vaccine">

    //<editor-fold desc="MMRstep1">

    val mmrVacValList = List(KpiConstants.mmrVacVal)
    val mmrVacCodeSystem = List(KpiConstants.cvxCodeVal)
    val joinedFormmrVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, mmrVacValList, mmrVacCodeSystem)
                                         .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormmrVacDf = joinedFormmrVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                            .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                            .select(s"df1.${KpiConstants.memberskColName}")
    //</editor-fold>

    //<editor-fold desc="MMRstep2">

    /*MR vaccine*/
    val mrVacValList = List(KpiConstants.mrVacVal)
    val mrVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedFormrVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, mrVacValList, mrVacCodeSystem)
                                        .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormrVacDf = joinedFormrVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                           .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                           .select(s"df1.${KpiConstants.memberskColName}")

    /*mumps valuelist*/
    val mumpsValList = List(KpiConstants.mumpsVal)
    val joinedFormumpsDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, mumpsValList, primaryDiagCodeSystem)
                                        .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormumpsDf = joinedFormumpsDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                          .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                          .select(s"df1.${KpiConstants.memberskColName}")

    val mumpsVacValList = List(KpiConstants.mumpsVacVal)
    val mumpsVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedFormumpsVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, mumpsVacValList, mumpsVacCodeSystem)
                                           .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormumpsVacDf = joinedFormumpsVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                .select(s"df1.${KpiConstants.memberskColName}")
    /*Rubella and Mumps Valueset*/
    val rubellaAndMumpsDf = (measrFormumpsDf.union(measrFormumpsVacDf)).intersect(measrFormrVacDf)
    //</editor-fold>

    //<editor-fold desc="MMRstep3">

    /*Measles valueset*/
    val measelesValList = List(KpiConstants.measelesVal)
    val joinedFormeaselesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, measelesValList, primaryDiagCodeSystem)
                                           .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormeaselesDf = joinedFormeaselesDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                .select(s"df1.${KpiConstants.memberskColName}")

    /*Measles Vaccine Administered valueset*/
    val measlesVacValList = List(KpiConstants.measlesVacVal)
    val measlesVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedFormeaslesDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, measlesVacValList, measlesVacCodeSystem)
                                          .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFormeaslesDf = joinedFormeaslesDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                              .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                              .select(s"df1.${KpiConstants.memberskColName}")

    /*Rubella Valueset*/
    val rubellaValList = List(KpiConstants.rubellaVal)
    val joinedForRubellaDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, rubellaValList, primaryDiagCodeSystem)
                                          .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrFForRubellaDf = joinedForRubellaDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                               .select(s"df1.${KpiConstants.memberskColName}")

    /*Rubella Vaccine Adminstred*/
    val rubellaVacValList = List(KpiConstants.rubellaVacVal)
    val rubellaVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForRubellaVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, rubellaVacValList, rubellaVacCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForRubellaVacDf = joinedForRubellaVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                    .select(s"df1.${KpiConstants.memberskColName}")


    val mmrstep3Df = (measrFormeaselesDf.union(measrFormeaslesDf)).intersect((measrFormumpsDf.union(measrFormumpsVacDf))).intersect((measrFForRubellaDf.union(measrForRubellaVacDf)))
    //</editor-fold>

    val mmrDf = measrFormmrVacDf.union(rubellaAndMumpsDf).union(mmrstep3Df)
    //</editor-fold>

    //<editor-fold desc="Atleast 3 HIB vaccine">

    val hibVacValList = List(KpiConstants.hibVacVal)
    val hibVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForhibVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, hibVacValList, hibVacCodeSystem)
                                         .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the ipv proceedures that has done after 42 days to 2nd dob*/
    val measrForhibVacDf = joinedForhibVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                .select("df1.*")

    /*atleast 3 ipv proceedure during the dob+42 to 2nd dob period*/
    val hibNumDf = measrForhibVacDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                   .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val))
                                   .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="HepatitisB">

    val hepatitisBValList = List(KpiConstants.hepatitisBVal)
    val joinedForhepatitisBDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, hepatitisBValList, primaryDiagCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForhepatitisBDf = joinedForhepatitisBDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                    .select(s"df1.${KpiConstants.memberskColName}")

    val hepatitisBVacValList = List(KpiConstants.hepatitisBVacVal)
    val hepatitisBVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForhepatitisBVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, hepatitisBVacValList, hepatitisBVacCodeSystem)
                                                .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForhepatitisBVacDf = joinedForhepatitisBVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                          .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                          .select(s"df1.${KpiConstants.memberskColName}")
    val atleast3HepAVacDf =  measrForhepatitisBVacDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                             .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val))
                                             .select(KpiConstants.memberskColName)

    val hepatitsBDf = measrForhepatitisBDf.union(atleast3HepAVacDf)
    //</editor-fold>

    //<editor-fold desc="VZV">

    val vzvValList = List(KpiConstants.vzvVal)
    val joinedForvzvDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, vzvValList, primaryDiagCodeSystem)
                                      .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForvzvDf = joinedForvzvDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                      .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                      .select(s"df1.${KpiConstants.memberskColName}")

    val vzvVacValList = List(KpiConstants.vzvVacVal)
    val vzvVacValCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForvzvVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, vzvVacValList, vzvVacValCodeSystem)
                                         .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForvzvVacDf = joinedForvzvVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                            .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                            .select(s"df1.${KpiConstants.memberskColName}")

    val vzvDf = measrForvzvDf.union(measrForvzvVacDf)
    //</editor-fold>

    //<editor-fold desc="Atleast 4 Pnemonical Conjugate">

    val pneConjVacValList = List(KpiConstants.pneConjVacVal)
    val pneConjVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForpneConjVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, pneConjVacValList, pneConjVacCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the pnemonical conjugate proceedures that has done after 42 days to 2nd dob*/
    val measrForpneConjVacDf = joinedForpneConjVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                            .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                            .select("df1.*")

    /*atleast 4 pnemonical conjugate proceedure during the dob+42 to 2nd dob period*/
    val pneConjVacNumDf = measrForpneConjVacDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count4Val))
                                              .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Hepatitis A">

    val hepatitisAValList = List(KpiConstants.hepatitisAVal)
    val joinedForhepatitisADf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, hepatitisAValList, primaryDiagCodeSystem)
                                             .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForhepatitisADf = joinedForhepatitisADf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                    .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                    .select(s"df1.${KpiConstants.memberskColName}")

    val hepatitisAVacValList = List(KpiConstants.hepatitisAVacVal)
    val hepatitisAVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForhepatitisAVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, hepatitisAVacValList, hepatitisAVacCodeSystem)
                                                .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    val measrForhepatitisAVacDf = joinedForhepatitisAVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                       .filter(($"df1.${KpiConstants.startDateColName}".>=($"df2.${KpiConstants.firstDobColName}")) && ($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.secondDobColName}")))
                                                       .select(s"df1.${KpiConstants.memberskColName}")

    val hepatitsADf = measrForhepatitisADf.union(measrForhepatitisAVacDf)
    //</editor-fold>

    //<editor-fold desc="Rotavirus">

    /*atleast 2 Rotavirus vaccine 2 dose*/
    val rotavirusVac2ValList = List(KpiConstants.rotavirusVac2Val)
    val rotavirusVacCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForrotavirusVac2Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, rotavirusVac2ValList, rotavirusVacCodeSystem)
                                               .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the pnemonical conjugate proceedures that has done after 42 days to 2nd dob*/
    val measrForrotavirusVac2Df = joinedForrotavirusVac2Df.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                        .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                        .select("df1.*")

    /*atleast 2 2dose rotavirus during the dob+42 to 2nd dob period*/
    val rotavirusVac2Df = measrForrotavirusVac2Df.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                              .select(KpiConstants.memberskColName)


    /*Atleast 3 Rotavirus vaccine 3 dose*/
    val rotavirusVac3ValList = List(KpiConstants.rotavirusVac3Val)
    val joinedForrotavirusVac3Df = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, rotavirusVac3ValList, rotavirusVacCodeSystem)
                                                .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the pnemonical conjugate proceedures that has done after 42 days to 2nd dob*/
    val measrForrotavirusVac3Df = joinedForrotavirusVac2Df.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                          .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                          .select("df1.*")

    /*atleast 3 3dose rotavirus during the dob+42 to 2nd dob period*/
    val rotavirusVac3Df = measrForrotavirusVac3Df.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                                 .filter($"${KpiConstants.countColName}".>=(KpiConstants.count3Val))
                                                 .select(KpiConstants.memberskColName)


    val atleast2_3doseDf = measrForrotavirusVac3Df.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                                  .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                                  .select(KpiConstants.memberskColName)

    val mesr2_3doseDf = measrForrotavirusVac3Df.as("df1").join(atleast2_3doseDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .select("df1.*")


    val rotovirus3Df = mesr2_3doseDf.as("df1").join(measrForrotavirusVac2Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                    .filter(abs(datediff($"df1.${KpiConstants.startDateColName}", $"df2.${KpiConstants.startDateColName}")).>(KpiConstants.days0))
                                    .select(s"df1.${KpiConstants.memberskColName}")

    val rotavirusNumDf = rotavirusVac2Df.union(rotavirusVac3Df).union(rotovirus3Df)
    //</editor-fold>

    //<editor-fold desc="Atleast 2 Influenza Vaccination">

    val influenzaVacValList = List(KpiConstants.influenzaVacVal)
    val influenzaVacCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinedForinfluenzaVacDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cisMeasureId, influenzaVacValList, influenzaVacCodeSystem)
                                               .select(KpiConstants.memberskColName, KpiConstants.startDateColName)

    /*membersk and start date for the pnemonical conjugate proceedures that has done after 42 days to 2nd dob*/
    val measrForinfluenzaVacDf = joinedForinfluenzaVacDf.as("df1").join(filterdForNumCalDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                        .filter(($"df1.${KpiConstants.startDateColName}".<=($"df1.${KpiConstants.secondDobColName}")) && ($"df1.${KpiConstants.startDateColName}".>=(date_add($"df2.${KpiConstants.dobColName}",KpiConstants.days42))))
                                                        .select("df1.*")

    /*atleast 4 pnemonical conjugate proceedure during the dob+42 to 2nd dob period*/
    val influenzaVacNumDf = measrForinfluenzaVacDf.groupBy(KpiConstants.memberskColName).agg(countDistinct($"${KpiConstants.startDateColName}").alias(KpiConstants.countColName))
                                                  .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                                  .select(KpiConstants.memberskColName)
    //</editor-fold>

    var numeratorDf = spark.emptyDataFrame
    var numeratorValueSet = List.empty[String]
     measureId match {

      case KpiConstants.cisDtpaMeasureId => numeratorDf = dtapNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList

      case KpiConstants.cisIpvMeasureId  => numeratorDf = ipvNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = ipvVaccineValList

      case KpiConstants.cisMmrMeasureId  => numeratorDf = mmrDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = mmrVacValList:::mrVacValList:::mumpsValList

      case KpiConstants.cisHiBMeasureId  => numeratorDf = hibNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = hibVacValList

      case KpiConstants.cisHepbMeasureId => numeratorDf = hepatitsBDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = hepatitisBValList:::hepatitisBVacValList

      case KpiConstants.cisVzvMeasureId  => numeratorDf = vzvDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = vzvValList:::vzvVacValList

      case KpiConstants.cisPneuMeasureId => numeratorDf = pneConjVacNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = pneConjVacValList

      case KpiConstants.cisHepaMeasureId => numeratorDf = hepatitsADf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = hepatitisAValList:::hepatitisAVacValList

      case KpiConstants.cisRotaMeasureId => numeratorDf = rotavirusNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = rotavirusVac2ValList:::rotavirusVac3ValList

      case KpiConstants.cisInflMeasureId => numeratorDf = influenzaVacNumDf.intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = influenzaVacValList

      case KpiConstants.cisCmb2MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb3MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb4MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(hepatitsADf).intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb5MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(rotavirusNumDf).intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb6MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(influenzaVacNumDf).intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb7MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(hepatitsADf).intersect(rotavirusNumDf)
                                                                   .intersect(dinominatorAfterExclDf)
                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb8MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(hepatitsADf).intersect(influenzaVacNumDf)
                                                                   .intersect(dinominatorAfterExclDf)
                                                          numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb9MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                   .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(rotavirusNumDf).intersect(influenzaVacNumDf)
                                                                   .intersect(dinominatorAfterExclDf)
                                                          numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

      case KpiConstants.cisCmb10MeasureId => numeratorDf = dtapNumDf.intersect(ipvNumDf).intersect(mmrDf).intersect(hibNumDf)
                                                                    .intersect(hepatitsBDf).intersect(vzvDf).intersect(pneConjVacNumDf).intersect(hepatitsADf).intersect(rotavirusNumDf)
                                                                    .intersect(influenzaVacNumDf).intersect(dinominatorAfterExclDf)
                                                            numeratorValueSet = dtapVacAdmValList:::ipvVaccineValList:::mmrVacValList:::hibVacValList:::hepatitisBValList

     }
    //</editor-fold>

    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val dinominatorExclValueSet = anaReactVacValList:::encephalVacValList:::disorderImmSysValList
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
