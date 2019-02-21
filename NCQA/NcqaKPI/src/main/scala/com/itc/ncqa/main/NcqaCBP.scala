package com.itc.ncqa.main

import com.itc.ncqa.Constants
import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType

import scala.collection.mutable


object NcqaCBP {

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

  /*  val conf = new SparkConf().setMaster("local[*]").setAppName("NCQACBP")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()*/
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

   /* val dimMemberDf = DataLoadFunctions.dataLoadFromHive(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, KpiConstants.memberidColName)

    /*loading fact claim and applying the filter condition*/
    val factClaimDf = DataLoadFunctions.dataLoadFromHive(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, KpiConstants.claimsidColName)
                                       .filter(($"${KpiConstants.serviceDateSkColName}".isNotNull)
                                              && (($"${KpiConstants.admitDateSkColName}".isNotNull && $"${KpiConstants.dischargeDateSkColName}".isNotNull) || ($"${KpiConstants.admitDateSkColName}".isNull && $"${KpiConstants.dischargeDateSkColName}".isNull)))

    val factRxClaimsDf = DataLoadFunctions.dataLoadFromHive(spark, KpiConstants.dbName, KpiConstants.factRxClaimTblName, data_source)

    /*Loading fact membership and applying filter logic*/
    val factMembershipDf = DataLoadFunctions.dataLoadFromHive(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
                                            .filter(($"${KpiConstants.memPlanStartDateSkColName}".isNotNull) && ($"${KpiConstants.memPlanEndDateSkColName}".isNotNull))
    val factMemAttrDf = DataLoadFunctions.dataLoadFromHive(spark,KpiConstants.dbName, KpiConstants.factMemAttrTblName,data_source)
    val factMonMemDf = DataLoadFunctions.dataLoadFromHive(spark,KpiConstants.dbName, KpiConstants.factMonMembershipTblName,data_source)
    val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
    val dimProviderDf = DataLoadFunctions.dataLoadFromHive(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
    val dimDateDf = DataLoadFunctions.dimDateLoadFunction(spark)
    val dimProductPlanDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName,KpiConstants.dimProductTblName,data_source)

*/
    val aLiat = List("col1")
    //val generalmembershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.generalmembershipTblName,aLiat)

    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))

    val visitDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,aLiat)
                                   .filter(($"${KpiConstants.serviceDateColName}".isNotNull)
                                       && (($"${KpiConstants.admitDateColName}".isNotNull && $"${KpiConstants.dischargeDateColName}".isNotNull)
                                            || ($"${KpiConstants.admitDateColName}".isNull && $"${KpiConstants.dischargeDateColName}".isNull)))



    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter($"${KpiConstants.measureIdColName}".===(KpiConstants.cbpMeasureId))

    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.cbpMeasureId))

    val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

    visitDf.cache()
    refHedisDf.cache()
    ref_medvaluesetDf.cache()
    membershipDf.cache()
   // println("counts:"+ visitDf.count()+","+ membershipDf.count()+","+ refHedisDf.count()+","+ref_medvaluesetDf.count())

    //</editor-fold>



    //<editor-fold desc="Eligible Population Calculation">


    //<editor-fold desc="Initial join function">

    val argmapForInitJoin = mutable.Map( KpiConstants.membershipTblName -> membershipDf, KpiConstants.visitTblName -> visitDf)
    val initialJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForInitJoin)

    initialJoinedDf.printSchema()
    //</editor-fold>

    /*Hospice removal*/

    val argmapforHospice = mutable.Map(KpiConstants.eligibleDfName -> initialJoinedDf, KpiConstants.refHedisTblName -> refHedisDf
                                      ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val hospiceValList = List(KpiConstants.hospiceVal)
    val hospiceCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val hospiceClaimsDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforHospice,hospiceValList,hospiceCodeSystem)
    val hospiceincurryearDf = UtilFunctions.measurementYearFilter(hospiceClaimsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                           .select(KpiConstants.memberidColName)
    val hospiceRemovedClaimsDf = initialJoinedDf.as("df1").join(hospiceincurryearDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                                .filter($"df2.${KpiConstants.memberidColName}".isNull)
                                                .select("df1.*")
    //<editor-fold desc="Age Filter">

    val ageFilterDf = UtilFunctions.ageFilter(hospiceRemovedClaimsDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age86Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
    ageFilterDf.printSchema()
    //</editor-fold>

    /*
    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">


    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberskColName, KpiConstants.benefitMedicalColname,
                                                  KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
                                                  KpiConstants.lobColName)
    val argMap = mutable.Map(KpiConstants.dateStartKeyName -> contEnrollStartDate, KpiConstants.dateEndKeyName -> contEnrollEndDate, KpiConstants.dateAnchorKeyName -> contEnrollEndDate,
                             KpiConstants.lobNameKeyName -> lob_name, KpiConstants.benefitKeyName -> KpiConstants.benefitMedicalColname)

    val contEnrollInDf = inputForContEnrolldf.withColumn(KpiConstants.contenrollLowCoName, lit(contEnrollStartDate).cast(DateType))
                                             .withColumn(KpiConstants.contenrollUppCoName, lit(contEnrollEndDate).cast(DateType))
                                             .withColumn(KpiConstants.anchorDateColName, lit(contEnrollEndDate).cast(DateType))


    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
                                                  || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))))



    val contEnrollStep2Df = contEnrollStep1Df.withColumn(KpiConstants.benefitMedicalColname, when($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal), 1).otherwise(0))




    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    val listDf = contEnrollStep2Df.groupBy($"${KpiConstants.memberskColName}")
                                  .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
                                       min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
                                       first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
                                       first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
                                  .filter(((date_add($"max_mem_end_date",KpiConstants.days45+1).>=($"${KpiConstants.contenrollUppCoName}"))
                                        && (date_sub($"min_mem_start_date",KpiConstants.days45 +1).<=($"${KpiConstants.contenrollLowCoName}"))))
                                  .select($"${KpiConstants.memberskColName}")

    val contEnrollStep3Df = contEnrollStep2Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                              .select("df1.*")


   // contEnrollStep3Df.printSchema()
    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberskColName}").orderBy(s"${KpiConstants.memStartDateColName}")


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep4Df = contEnrollStep3Df.withColumn(KpiConstants.datediffColName, datediff(lead($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
                                             .withColumn(KpiConstants.anchorflagColName,when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
                                                                                           && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}"))
                                                                                           && ($"${KpiConstants.lobColName}".===(lob_name)), lit(1)).otherwise(lit(0)))
                                             .withColumn(KpiConstants.countColName, when($"${KpiConstants.datediffColName}".>(1),lit(1)).otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep4Df.groupBy(KpiConstants.memberskColName)
                                             .agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
                                                  max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
                                                  max($"${KpiConstants.datediffColName}").alias(KpiConstants.maxDateDiffColName),
                                                  sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
                                                  sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName),
                                                  first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
                                                  first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
                                                  sum($"${KpiConstants.benefitMedicalColname}").alias(KpiConstants.sumBenefitColName),
                                                  count($"${KpiConstants.benefitMedicalColname}").alias(KpiConstants.countBenefitColName))



    val contEnrollmemDf = contEnrollStep5Df.filter((($"${KpiConstants.maxDateDiffColName}" - 1).<=(KpiConstants.days45) || ($"${KpiConstants.maxDateDiffColName}" - 1).isNull)
                                                     && ($"${KpiConstants.anchorflagColName}".>(0))
                                                     && ((($"${KpiConstants.countColName}")
                                                         + (when(date_sub($"min_mem_start_date", 1).>($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
                                                         + (when(date_add($"max_mem_end_date", 1).<($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
                                                     && ($"${KpiConstants.sumBenefitColName}".===($"${KpiConstants.countBenefitColName}")))
                                           .select(KpiConstants.memberskColName)


    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                  .select("df1.*")

    //contEnrollDf.printSchema()

   contEnrollDf.cache()

    //</editor-fold>

    //<editor-fold desc="Eligible Event calculation">

    val argmapForEligibleEvent = mutable.Map(KpiConstants.eligibleDfName -> contEnrollDf , KpiConstants.factClaimTblName -> factClaimDf,
                                             KpiConstants.refHedisTblName ->refHedisDf , KpiConstants.dimDateTblName -> dimDateDf)


    //<editor-fold desc="Essential Hyper Tension">

    /*Essential Hyper Tension As primary diagnosis*/
    val essHypTenValList = List(KpiConstants.essentialHyptenVal)
    val primaryDiagCodeVal = List(KpiConstants.icdCodeVal)
    val joinForEssHypTenAsDiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForEligibleEvent, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, essHypTenValList, primaryDiagCodeVal)
    val essentialHypTenDf = UtilFunctions.measurementYearFilter(joinForEssHypTenAsDiagDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                         .select(KpiConstants.memberskColName,KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV ">

    val outPatwoUbrevValList = List(KpiConstants.outpatwoUbrevVal)
    val outPatwoUbrevCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinForOutpatWoUbrevDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForEligibleEvent,KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,outPatwoUbrevValList,outPatwoUbrevCodeSystem)
    val outpatientWoUbrevDf = UtilFunctions.measurementYearFilter(joinForOutpatWoUbrevDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                           .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Telehealth Modifier">

    val teleHealthModValList = List(KpiConstants.telehealthModifierVal)
    val teleHealthModCodeSystem = List(KpiConstants.modifierCodeVal)
    val joinedForTeleHealthModDf =  UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForEligibleEvent,KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,teleHealthModValList,teleHealthModCodeSystem)
    val teleHealthModDf = UtilFunctions.measurementYearFilter(joinedForTeleHealthModDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                       .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Telephone visits">

    val telephoneVistValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVistCodeSystem = List(KpiConstants.cptCodeVal)
    val joinFortelephoneVistDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForEligibleEvent, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, telephoneVistValList,telephoneVistCodeSystem)
    val telephoneVistDf = UtilFunctions.measurementYearFilter(joinFortelephoneVistDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                       .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Online Assesment">

    val onlineassesValList = List(KpiConstants.onlineAssesmentVal)
    val onlineassesCodeSystem = List(KpiConstants.cptCodeVal)
    val joinForonlineassesDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForEligibleEvent, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, onlineassesValList, onlineassesCodeSystem)
    val onlineassesmentDf = UtilFunctions.measurementYearFilter(joinForonlineassesDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                         .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Event1">

    val event1_1Df = (outpatientWoUbrevDf.except(teleHealthModDf)).intersect(essentialHypTenDf)

    val event1_2Df = telephoneVistDf.intersect(essentialHypTenDf)

    val event1Df = event1_1Df.as("df1").join(event1_2Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                              .select(KpiConstants.memberskColName,KpiConstants.secondDiagColName)

    //</editor-fold>

    //<editor-fold desc="Event2">

    val event2df = event1_1Df.as("df1").join(event1_1Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                              .select(KpiConstants.memberskColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    //<editor-fold desc="Event3">

    val event3_2Df = (outpatientWoUbrevDf.intersect(teleHealthModDf)).intersect(essentialHypTenDf)

    val event3Df  = event1_1Df.as("df1").join(event3_2Df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                               .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                               .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                               .select(KpiConstants.memberskColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    //<editor-fold desc="Event4">

    val event4_2df = onlineassesmentDf.intersect(essentialHypTenDf)

    val event4Df = event1_1Df.as("df1").join(event4_2df.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                              .select(KpiConstants.memberskColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    val eventInDf = event1Df.union(event2df).union(event3Df).union(event4Df)

    val eventDf = eventInDf.groupBy(KpiConstants.memberskColName).agg(min(KpiConstants.secondDiagColName).alias(KpiConstants.secondDiagColName))

    val totalPopulationDf = contEnrollDf.as("df1").join(eventDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.innerJoinType)
                                                        .select("df1.*", s"df2.${KpiConstants.secondDiagColName}")

    totalPopulationDf.printSchema()
    //totalPopulationDf.cache()
    //</editor-fold>


    //<editor-fold desc="Mandatory Exclusion Calculation">

    val argmapForMandExcl = mutable.Map(KpiConstants.eligibleDfName -> totalPopulationDf, KpiConstants.factClaimTblName -> factClaimDf,
                                        KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf,
                                        KpiConstants.factRxClaimTblName -> factRxClaimsDf, KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Mandatory Exclusion1">

    val hospiceDf = UtilFunctions.hospiceExclusionFunction(spark, argmapForMandExcl,year)
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion2(Only for Medicare)">

    val ageCheckDate = year + "12-31"
    val mandatoryExcl2Df = contEnrollDf.filter(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName)) && (($"${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))
                                              || ($"${KpiConstants.ltiFlagColName}".===(KpiConstants.boolTrueVal))) && (add_months($"${KpiConstants.dobColName}",KpiConstants.months792).<=(ageCheckDate)))
                                       .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion3">

    val mandatoryExcl3Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age81Val, KpiConstants.age120Val)
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion4">

    val fralityAndAgeGt66Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age66Val, KpiConstants.age120Val)

    //<editor-fold desc="Advanced Illness valueset">

    val advillValList = List(KpiConstants.advancedIllVal)
    val joinedForAdvancedIllDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,advillValList,primaryDiagCodeVal)
    val advancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                     .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Outpatient Valueset">

    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForOutPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,outPatValList,outPatCodeSystem)
    val outPatientDf = UtilFunctions.measurementYearFilter(joinedForOutPatDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                    .select(KpiConstants.memberskColName,KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Observation Valueset">

    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val joinedForObservationDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,obsVisitValList,obsVisitCodeSystem)
    val observationDf = UtilFunctions.measurementYearFilter(joinedForObservationDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                     .select(KpiConstants.memberskColName,KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Ed Valueset">

    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForEdVisistsDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,edVisitValList,edVisitCodeSystem)
    val edVisitsDf = UtilFunctions.measurementYearFilter(joinedForEdVisistsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                  .select(KpiConstants.memberskColName,KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Non Acutr InPatient Valueset">

    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val joinedForNonAcutePatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,nonAcuteInValList,nonAcuteInCodeSsytem)
    val nonAcutePatDf = UtilFunctions.measurementYearFilter(joinedForNonAcutePatDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement2Val)
                                     .select(KpiConstants.memberskColName,KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Acute InPatient Valueset">

    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val joinedForAcuteInpatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.proceedureCodeColName,KpiConstants.innerJoinType,KpiConstants.cbpMeasureId,acuteInPatValLiat,acuteInPatCodeSystem)
    val acuteInpatDf = UtilFunctions.measurementYearFilter(joinedForAcuteInpatDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                    .select(KpiConstants.memberskColName, KpiConstants.serviceDateColName)
    //</editor-fold>

    //<editor-fold desc="Dementia Medication List">

    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val joinedForDemMedDf = UtilFunctions.dimMemberFactRxclaimsHedisJoinFunction(spark,argmapForMandExcl,KpiConstants.cbpMeasureId,dementiaMedValList)
    val dementiaMedDf = UtilFunctions.mesurementYearFilter(joinedForDemMedDf, KpiConstants.rxServiceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                     .select(KpiConstants.memberskColName)
    //</editor-fold>



    val outPatAndAdvIllDf = outPatientDf.intersect(advancedIllDf)

    val obsAndAdvIllDf = observationDf.intersect(advancedIllDf)

    val edAndAdvIllDf = edVisitsDf.intersect(advancedIllDf)

    val nonacuteAndAdvIllDf = nonAcutePatDf.intersect(advancedIllDf)


    val mandatoryExcl4_2Df = outPatAndAdvIllDf.union(obsAndAdvIllDf).union(edAndAdvIllDf).union(nonacuteAndAdvIllDf)
                                              .groupBy(KpiConstants.memberskColName)
                                              .agg(countDistinct($"${KpiConstants.serviceDateColName}").alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                              .select(KpiConstants.memberskColName)

    val mandatoryExcl4_3Df = acuteInpatDf.intersect(advancedIllDf).select(KpiConstants.memberskColName)

    val mandatoryExcl4Df = (fralityAndAgeGt66Df.intersect(mandatoryExcl4_2Df)).union(mandatoryExcl4_3Df).union(dementiaMedDf)
    //</editor-fold>


    var mandatoryExclDf = spark.emptyDataFrame

    if(KpiConstants.medicareLobName.equalsIgnoreCase(lob_name)){

      mandatoryExclDf = hospiceDf.union(mandatoryExcl2Df).union(mandatoryExcl3Df).union(mandatoryExcl4Df)

    }
    else{

      mandatoryExclDf = hospiceDf.union(mandatoryExcl2Df).union(mandatoryExcl3Df).union(mandatoryExcl4Df)

    }
    mandatoryExclDf.cache()
    //</editor-fold>

     val eligibleDf =  totalPopulationDf.as("df1").join(mandatoryExclDf.as("df2"), $"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}", KpiConstants.leftOuterJoinType)
                                                         .filter($"df2.${KpiConstants.memberskColName}".===(null))
                                                         .select(s"df1.${KpiConstants.memberskColName}", s"df1.${KpiConstants.secondDiagColName}")

    //</editor-fold>

    //<editor-fold desc="Denominator Calculation">

    val denominatorDf = eligibleDf
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val argmapForOptExclusion = mutable.Map(KpiConstants.eligibleDfName -> eligibleDf, KpiConstants.factClaimTblName -> factClaimDf,
                                            KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf,
                                            KpiConstants.factRxClaimTblName -> factRxClaimsDf, KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Optional Exclusion1(ESRD, ESRD Obsolent and kidney transplant)">

    /*Exclusions for primaryDiagnosisCodeSystem with valueset "ESRD","Kidney Transplant" */
    val esrdKidneyTrdiagValList = List(KpiConstants.esrdVal, KpiConstants.kidneyTransplantVal)
    val joinForesrdKidneyTrdiagDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForOptExclusion, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, esrdKidneyTrdiagValList, primaryDiagCodeVal)
    val mesurForesrdKidneyTrdiagDf = UtilFunctions.measurementYearFilter(joinForesrdKidneyTrdiagDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                                  .select(KpiConstants.memberskColName)

    /*Exclusions for proceedureCodeSystem with valueset "ESRD","ESRD Obsolete","Kidney Transplant" */
    val esrdKidneyTrProcValList = List(KpiConstants.esrdVal, KpiConstants.esrdObsoleteVal, KpiConstants.kidneyTransplantVal)
    val esrdKidneyTrProcCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.posCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val joinForesrdKidneyTrProcDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForOptExclusion, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, esrdKidneyTrProcValList, esrdKidneyTrProcCodeSystem)
    val measurForesrdKidneyTrProcDf = UtilFunctions.measurementYearFilter(joinForesrdKidneyTrProcDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                                   .select(KpiConstants.memberskColName)

    val optionalExcl1Df = mesurForesrdKidneyTrdiagDf.union(measurForesrdKidneyTrProcDf)
    //</editor-fold>

    //<editor-fold desc="Optional exclusion2(pregnancy Exclusion)">

    val pregnancyValList = List(KpiConstants.pregnancyVal)
    val joinedForPregnancyDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,argmapForOptExclusion,KpiConstants.primaryDiagnosisColname,KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,pregnancyValList,primaryDiagCodeVal)
    val pregnancyDf = UtilFunctions.measurementYearFilter(joinedForPregnancyDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement0Val)
      .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion3(Inpatient and Nonacute Inpatient)">

    //<editor-fold desc="Inpatient">

    val inPatientValList = List(KpiConstants.inpatientStayVal)
    val inPatientCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinForinPatientDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForOptExclusion, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, inPatientValList, inPatientCodeSystem)
    val measurinPatientDf = UtilFunctions.measurementYearFilter(joinForinPatientDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                         .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Non acute Inpatient">

    val nonAcuteInPatValList = List(KpiConstants.nonacuteInPatStayVal)
    val nonAcuteInPatCodeSystem = List(KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val joinFornonAcuteInPatDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForOptExclusion, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, nonAcuteInPatValList, nonAcuteInPatCodeSystem)
    val measurnonAcuteInPatDf = UtilFunctions.measurementYearFilter(joinFornonAcuteInPatDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                             .select(KpiConstants.memberskColName)
    //</editor-fold>

    val optionalExcl3Df = measurinPatientDf.intersect(measurnonAcuteInPatDf)
    //</editor-fold>

    val optionalExclDf = 	optionalExcl1Df.union(pregnancyDf).union(optionalExcl3Df)
    optionalExclDf.cache()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val argmapForNumerator = mutable.Map(KpiConstants.eligibleDfName -> eligibleDf, KpiConstants.factClaimTblName -> factClaimDf,
                                         KpiConstants.refHedisTblName -> refHedisDf, KpiConstants.dimDateTblName -> dimDateDf,
                                         KpiConstants.factRxClaimTblName -> factRxClaimsDf, KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)


    //<editor-fold desc=" Outpatient Without UBREV,Nonacute Inpatient,Remote Blood Pressure Monitoring valueset">

    val outpatnonacuterebpValList = List(KpiConstants.outpatwoUbrevVal, KpiConstants.nonAcuteInPatientVal,KpiConstants.remotebpmVal)
    val outpatnonacuterebpCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinForoutpatnonacuterebpDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForNumerator, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId,outpatnonacuterebpValList, outpatnonacuterebpCodeSystem)
    val outpatnonacuterebpDf = UtilFunctions.measurementYearFilter(joinForoutpatnonacuterebpDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                            .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))
                                            .groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                       first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                            .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                            .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="bpReading (Systolic Less Than 140, Diastolic Less Than 80,Diastolic 80–89 valueset)">

    val sysbpReadingValList = List(KpiConstants.systolicLt140Val)
    val sysbpReadingCodeSystem = List(KpiConstants.cptCatIIVal)
    val joinForsysbpReadingDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForNumerator, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, sysbpReadingValList, sysbpReadingCodeSystem)
    val sysbpReadingDf = UtilFunctions.measurementYearFilter(joinForsysbpReadingDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                            .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))
                                            .groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                       first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                            .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                            .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Diastolic 80-90 and less than 80">

    val diasValList = List(KpiConstants.diastolicLt80Val, KpiConstants.diastolicBtwn8090Val)
    val diasCodeSystem = List(KpiConstants.cptCatIIVal)
    val joinFordiasbpReadingDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark, argmapForNumerator, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, diasValList, diasCodeSystem)
    val diasbpReadingDf = UtilFunctions.measurementYearFilter(joinFordiasbpReadingDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                       .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))
                                       .groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                  first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                       .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                       .select(KpiConstants.memberskColName)

    //</editor-fold>

    val numeratorDf = outpatnonacuterebpDf.intersect(sysbpReadingDf).intersect(diasbpReadingDf)
    numeratorDf.cache()
    //</editor-fold>


    /*Ncqa Output Creation Code*/
    val dfMapForNcqaOut = mutable.Map(KpiConstants.totalPopDfName -> totalPopulationDf , KpiConstants.eligibleDfName -> eligibleDf.select(KpiConstants.memberskColName),
                                      KpiConstants.mandatoryExclDfname -> mandatoryExclDf , KpiConstants.optionalExclDfName -> optionalExclDf,
                                      KpiConstants.numeratorDfName -> numeratorDf)

*/
    /*
    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    val numeratorValueSet = outpatnonacuterebpValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numExclValueSet = KpiConstants.emptyList
    val outValueSetForOutput = List(numeratorValueSet, dinominatorExclValueSet, numExclValueSet)
    val sourceAndMsrList = List(data_source,measureId)


    val numExclDf = spark.emptyDataFrame


    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outValueSetForOutput, sourceAndMsrList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>
*/
    spark.sparkContext.stop()

  }

}
