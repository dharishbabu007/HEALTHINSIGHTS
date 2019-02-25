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
import org.bouncycastle.asn1.x509.KeyPurposeId

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

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

    val aLiat = List("col1")
    //val generalmembershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.generalmembershipTblName,aLiat)

    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))
                                        .cache()

    val visitDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,aLiat)
                                   .filter(($"${KpiConstants.serviceDateColName}".isNotNull)
                                       && (($"${KpiConstants.admitDateColName}".isNotNull && $"${KpiConstants.dischargeDateColName}".isNotNull)
                                            || ($"${KpiConstants.admitDateColName}".isNull && $"${KpiConstants.dischargeDateColName}".isNull)))
                                   .cache()



    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter($"${KpiConstants.measureIdColName}".===(KpiConstants.cbpMeasureId))
                                      .cache()

    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.cbpMeasureId))
                                             .cache()

    //</editor-fold>

    //<editor-fold desc="Eligible Population Calculation">


    //<editor-fold desc="Initial join function">

    val argmapForInitJoin = mutable.Map( KpiConstants.membershipTblName -> membershipDf, KpiConstants.visitTblName -> visitDf)
    val initialJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForInitJoin).cache()
    initialJoinedDf.count()
    membershipDf.unpersist()
    visitDf.unpersist()
   // println("initialJoinedDf.count():"+initialJoinedDf.count())
    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val argmapforHospice = mutable.Map(KpiConstants.eligibleDfName -> initialJoinedDf, KpiConstants.refHedisTblName -> refHedisDf
                                      ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val hospiceValList = List(KpiConstants.hospiceVal)
    val hospiceCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val hospiceClaimsDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforHospice,hospiceValList,hospiceCodeSystem)
    val hospiceincurryearDf = UtilFunctions.measurementYearFilter(hospiceClaimsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                           .select(KpiConstants.memberidColName)

    var hospiceRemovedClaimsDf = spark.emptyDataFrame
    if (hospiceincurryearDf.count()> 0){
      hospiceRemovedClaimsDf = initialJoinedDf.as("df1").join(hospiceincurryearDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                              .filter($"df2.${KpiConstants.memberidColName}".isNull)
                                              .select("df1.*").cache()
    }
    else {
      hospiceRemovedClaimsDf = initialJoinedDf.cache()
    }
    hospiceRemovedClaimsDf.count()
    initialJoinedDf.unpersist()
    //</editor-fold>

    //<editor-fold desc="Age Filter">

    val ageFilterDf = UtilFunctions.ageFilter(hospiceRemovedClaimsDf, KpiConstants.dobColName, year, KpiConstants.age18Val, KpiConstants.age86Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)
                                   .cache()
    ageFilterDf.count()
    hospiceRemovedClaimsDf.unpersist()
    //println("ageFilterDf.count():"+ageFilterDf.count())
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">


    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
                                                  KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
                                                  KpiConstants.lobColName)

    val contEnrollInDf = inputForContEnrolldf.withColumn(KpiConstants.contenrollLowCoName, lit(contEnrollStartDate).cast(DateType))
                                             .withColumn(KpiConstants.contenrollUppCoName, lit(contEnrollEndDate).cast(DateType))
                                             .withColumn(KpiConstants.anchorDateColName, lit(contEnrollEndDate).cast(DateType))


    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
                                                  || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))))



    val contEnrollStep2Df = contEnrollStep1Df.withColumn(KpiConstants.benefitMedicalColname, when($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal), 1).otherwise(0))




    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    val listDf = contEnrollStep2Df.groupBy($"${KpiConstants.memberidColName}")
                                  .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
                                       min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
                                       first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
                                       first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
                                  .filter(((date_add($"max_mem_end_date",KpiConstants.days45+1).>=($"${KpiConstants.contenrollUppCoName}"))
                                        && (date_sub($"min_mem_start_date",KpiConstants.days45 +1).<=($"${KpiConstants.contenrollLowCoName}"))))
                                  .select($"${KpiConstants.memberidColName}")

    val contEnrollStep3Df = contEnrollStep2Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                              .select("df1.*")


   // contEnrollStep3Df.printSchema()
    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberidColName}").orderBy(s"${KpiConstants.memStartDateColName}")


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep4Df = contEnrollStep3Df.withColumn(KpiConstants.datediffColName, datediff(lead($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
                                             .withColumn(KpiConstants.anchorflagColName,when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
                                                                                           && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}"))
                                                                                           && ($"${KpiConstants.lobColName}".===(lob_name)), lit(1)).otherwise(lit(0)))
                                             .withColumn(KpiConstants.countColName, when($"${KpiConstants.datediffColName}".>(1),lit(1)).otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep4Df.groupBy(KpiConstants.memberidColName)
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
                                           .select(KpiConstants.memberidColName)


    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                  .select("df1.*").cache()
    //val table1 = contEnrollDf.createTempView("contEnrollTable")
    println(contEnrollDf.count())
    ageFilterDf.unpersist()
    //contEnrollDf.printSchema()
    //println("contEnroll count:"+contEnrollDf.count())

    //</editor-fold>

    //<editor-fold desc="Eligible Event calculation">

    val contEnrollNonSupplDf = contEnrollDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
   // println("contEnrollNonSupplDf:"+ contEnrollNonSupplDf.count())
    val baseargMap = mutable.Map(KpiConstants.eligibleDfName -> contEnrollNonSupplDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                 KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Outpatient without UBREV ">

    val outPatwoUbrevValList = List(KpiConstants.outpatwoUbrevVal)
    val outPatwoUbrevCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal)
    val joinForOutpatWoUbrevDf = UtilFunctions.joinWithRefHedisFunction(spark, baseargMap,outPatwoUbrevValList,outPatwoUbrevCodeSystem)
    val outpatientWoUbrevDf = UtilFunctions.measurementYearFilter(joinForOutpatWoUbrevDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)
                                           .cache()



    //println("outpatientWoUbrevDf:"+ outpatientWoUbrevDf.count())

    //</editor-fold>


    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension">

    val outpatessargMap= mutable.Map(KpiConstants.eligibleDfName -> outpatientWoUbrevDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                                    KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)
    val essHypTenValList = List(KpiConstants.essentialHyptenVal)
    val essCodeSsytem = List(KpiConstants.icd10cmCodeVal)
    val outpatwessentialHyptensDf = UtilFunctions.joinWithRefHedisFunction(spark, outpatessargMap,  essHypTenValList, essCodeSsytem)
                                                 .cache()

    outpatwessentialHyptensDf.count()

    println("outpatwessentialHyptensDf.storageLevel:"+outpatwessentialHyptensDf.storageLevel)

    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension with TeleHealth Modifier">

    val outpatesswtelhealthargMap = mutable.Map(KpiConstants.eligibleDfName -> outpatwessentialHyptensDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                                KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val teleHealthModValList = List(KpiConstants.telehealthModifierVal)
    val teleHealthModCodeSystem = List(KpiConstants.modifierCodeVal)
    val optesshyptentelheaModDf =  UtilFunctions.joinWithRefHedisFunction(spark, outpatesswtelhealthargMap,teleHealthModValList,teleHealthModCodeSystem)
                                                .cache()

    optesshyptentelheaModDf.count()
    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension without Telehealth Modifier">


    val opesshyptenwotelheaDf =  outpatwessentialHyptensDf.as("df1").join(refHedisDf.as("df2"), $"df1.${KpiConstants.proccodemod1ColName}" =!=  $"df2.${KpiConstants.codeColName}"
      && $"df1.${KpiConstants.proccodemod2ColName}"=!= $"df2.${KpiConstants.codeColName}"
      && $"df1.${KpiConstants.proccode2mod1ColName}" =!= $"df2.${KpiConstants.codeColName}"
      && $"df1.${KpiConstants.proccodemod2ColName}" =!= $"df2.${KpiConstants.codeColName}",KpiConstants.innerJoinType)
      .filter($"df2.${KpiConstants.valuesetColName}".isin(teleHealthModValList:_*))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.serviceDateColName}").cache()
    opesshyptenwotelheaDf.count()

    //</editor-fold>

    //<editor-fold desc="Telephone visits">

    val telephoneVistValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVistCodeSystem = List(KpiConstants.cptCodeVal)
    val joinFortelephoneVistDf = UtilFunctions.joinWithRefHedisFunction(spark, baseargMap, telephoneVistValList,telephoneVistCodeSystem)
    val telephoneVistDf = UtilFunctions.measurementYearFilter(joinFortelephoneVistDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)


    //</editor-fold>

    //<editor-fold desc="Telephone visits with Essential Hypertension">

    val telephoneessargMap= mutable.Map(KpiConstants.eligibleDfName -> telephoneVistDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val telephoneEssentialHypDf = UtilFunctions.joinWithRefHedisFunction(spark, telephoneessargMap, essHypTenValList,essCodeSsytem)
                                               .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)
    telephoneEssentialHypDf.cache()
    telephoneEssentialHypDf.count()
    //</editor-fold>

    //<editor-fold desc="Online Assesment">

    val onlineassesValList = List(KpiConstants.onlineAssesmentVal)
    val onlineassesCodeSystem = List(KpiConstants.cptCodeVal)
    val joinForonlineassesDf = UtilFunctions.joinWithRefHedisFunction(spark,baseargMap, onlineassesValList, onlineassesCodeSystem)
    val onlineassesmentDf = UtilFunctions.measurementYearFilter(joinForonlineassesDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)

    //</editor-fold>

    //<editor-fold desc="Online Assesment with Essential Hypertension">

    val onlineessargMap= mutable.Map(KpiConstants.eligibleDfName -> onlineassesmentDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                     KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val onlineEssentialHypDf = UtilFunctions.joinWithRefHedisFunction(spark, onlineessargMap, essHypTenValList,essCodeSsytem)
                                               .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)
    //</editor-fold>


    //<editor-fold desc="Event1">

    opesshyptenwotelheaDf.printSchema()

    telephoneEssentialHypDf.printSchema()

    val event1Df = opesshyptenwotelheaDf.select(s"${KpiConstants.memberidColName}", s"${KpiConstants.serviceDateColName}").as("df1").join(telephoneEssentialHypDf.select(s"${KpiConstants.memberidColName}", s"${KpiConstants.serviceDateColName}").as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                         .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                                         .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                                    .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                                         .select(s"df1.${KpiConstants.memberidColName}",KpiConstants.secondDiagColName)

    //println("printing event1Df.count():"+event1Df.count())
    //event1Df.printSchema()
    //</editor-fold>


    //<editor-fold desc="Event2">

    val event2df = opesshyptenwotelheaDf.as("df1").join(opesshyptenwotelheaDf.as("df2"), KpiConstants.memberidColName)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                              .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    //<editor-fold desc="Event3">

    val event3Df  = opesshyptenwotelheaDf.as("df1").join(optesshyptentelheaModDf.as("df2"), KpiConstants.memberidColName)
                                               .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                               .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                               .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    //<editor-fold desc="Event4">

    val event4Df = opesshyptenwotelheaDf.as("df1").join(onlineEssentialHypDf.as("df2"), KpiConstants.memberidColName)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.${KpiConstants.serviceDateColName}"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.${KpiConstants.serviceDateColName}"))
                                              .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)
    //</editor-fold>

    val eventInDf = event1Df.union(event2df).union(event3Df).union(event4Df)

    val eventDf = eventInDf.groupBy(KpiConstants.memberidColName).agg(min(KpiConstants.secondDiagColName).alias(KpiConstants.secondDiagColName))

    val totalPopulationDf = contEnrollDf.as("df1").join(eventDf.as("df2"), KpiConstants.memberidColName)
                                                         .select("df1.*", s"df2.${KpiConstants.secondDiagColName}")

    //totalPopulationDf.printSchema()

    //</editor-fold>


    //<editor-fold desc="Mandatory Exclusion Calculation">

    val argmapForMandExcl = mutable.Map(KpiConstants.eligibleDfName -> totalPopulationDf, KpiConstants.refHedisTblName -> refHedisDf,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)



    //<editor-fold desc="Mandatory Exclusion1(Only for Medicare)">

    val ageCheckDate = year + "12-31"
    val mandatoryExcl2Df = contEnrollDf.filter(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName)) && (($"${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))
                                              || ($"${KpiConstants.ltiFlagColName}".===(KpiConstants.boolTrueVal))) && (add_months($"${KpiConstants.dobColName}",KpiConstants.months792).<=(ageCheckDate)))
                                       .select(KpiConstants.memberskColName)
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion2">

    val mandatoryExcl3Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age81Val, KpiConstants.age120Val)
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion3">

    val fralityAndAgeGt66Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age66Val, KpiConstants.age120Val)
                                           .select(KpiConstants.memberidColName)



    //<editor-fold desc="Advanced Illness valueset">

    val advillValList = List(KpiConstants.advancedIllVal)
    val advillCodeSystem = List(KpiConstants.icd10cmCodeVal)
    val joinedForAdvancedIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapForMandExcl,advillValList,advillCodeSystem)
    val advancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)

    //</editor-fold>

    val argMapwithadvill = mutable.Map(KpiConstants.eligibleDfName -> advancedIllDf, KpiConstants.refHedisTblName -> refHedisDf,
                                      KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Outpatient Valueset with Advanced Illness">

    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val outpatAndAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,outPatValList,outPatCodeSystem)
                                         .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Observation Valueset with Advanced Illness">

    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = List(KpiConstants.cptCodeVal)
    val observationAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,obsVisitValList,obsVisitCodeSystem)
                                     .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Ed Valueset with Advanced Illness">

    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val edVisitsAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,edVisitValList,edVisitCodeSystem)
                                        .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Non Acutr InPatient Valueset with Advanced Illness">

    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.ubrevCodeVal)
    val nonAcuteInPatAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,nonAcuteInValList,nonAcuteInCodeSsytem)

    //</editor-fold>

    //<editor-fold desc="Acute InPatient Valueset with Advanced Illness">

    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.ubrevCodeVal)
    val acuteInpatAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,acuteInPatValLiat,acuteInPatCodeSystem)
                                          .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="Dementia Medication List">

    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val dementiaCodeSystem = List(KpiConstants.rxnormCodeVal)
    val joinedForDemMedDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapForMandExcl,dementiaMedValList,dementiaCodeSystem)
    val dementiaMedDf = UtilFunctions.mesurementYearFilter(joinedForDemMedDf, KpiConstants.rxServiceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                     .select(KpiConstants.memberidColName)
    //</editor-fold>



    val mandatoryExcl4_2Df = outpatAndAdvIllDf.union(observationAdvIllDf).union(edVisitsAdvIllDf).union(nonAcuteInPatAdvIllDf)
                                              .groupBy(KpiConstants.memberidColName)
                                              .agg(countDistinct($"${KpiConstants.serviceDateColName}").alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                              .select(KpiConstants.memberidColName)


    val mandatoryExcl4Df = fralityAndAgeGt66Df.intersect((mandatoryExcl4_2Df.union(acuteInpatAdvIllDf).union(dementiaMedDf)))
    //</editor-fold>


    var mandatoryExclDf = spark.emptyDataFrame

    if(KpiConstants.medicareLobName.equalsIgnoreCase(lob_name)){

      mandatoryExclDf = mandatoryExcl2Df.union(mandatoryExcl3Df).union(mandatoryExcl4Df)

    }
    else{

      mandatoryExclDf = mandatoryExcl3Df.union(mandatoryExcl4Df)

    }
    mandatoryExclDf.cache()
    //</editor-fold>

     val eligibleDf =  totalPopulationDf.as("df1").join(mandatoryExclDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                                         .filter($"df2.${KpiConstants.memberidColName}".===(null))
                                                         .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.secondDiagColName}")

    //</editor-fold>

    //<editor-fold desc="Denominator Calculation">

    val denominatorDf = eligibleDf
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val inputForOptExclAndNumDf = contEnrollDf.as("df1").join(eligibleDf.as("df2"), KpiConstants.memberidColName)
                                        .select("df1.*")

    val argmapForOptExclusion = mutable.Map(KpiConstants.eligibleDfName -> inputForOptExclAndNumDf, KpiConstants.refHedisTblName -> refHedisDf,
                                            KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Optional Exclusion1(ESRD, ESRD Obsolent and kidney transplant)">

    val esrdobsKidneyTrValList = List(KpiConstants.esrdVal, KpiConstants.esrdObsoleteVal, KpiConstants.kidneyTransplantVal)
    val esrdobsKidneyTrCodeSsytem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.icd10cmCodeVal,
                                         KpiConstants.icd10pcsCodeVal, KpiConstants.icd9cmCodeVal, KpiConstants.icd9pcsCodeVal,
                                         KpiConstants.posCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)

    val joinForesrdobsKidneyTrDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapForOptExclusion, esrdobsKidneyTrValList, esrdobsKidneyTrCodeSsytem)
    val esrdobsKidneyTrDf = UtilFunctions.measurementYearFilter(joinForesrdobsKidneyTrDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                                  .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Optional exclusion2(pregnancy Exclusion)">

    val pregnancyValList = List(KpiConstants.pregnancyVal)
    val pregnancyCodeSsytem = List(KpiConstants.icd10cmCodeVal)
    val joinedForPregnancyDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapForOptExclusion,pregnancyValList,pregnancyCodeSsytem)
    val pregnancyDf = UtilFunctions.measurementYearFilter(joinedForPregnancyDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement0Val)
                                   .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion3(Inpatient and Nonacute Inpatient)">

    //<editor-fold desc="Inpatient">

    val inPatientValList = List(KpiConstants.inpatientStayVal)
    val inPatientCodeSystem = List(KpiConstants.ubrevCodeVal)
    val joinForinPatientDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapForOptExclusion, inPatientValList, inPatientCodeSystem)
    val inPatientstayDf = UtilFunctions.measurementYearFilter(joinForinPatientDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)

    //</editor-fold>

    //<editor-fold desc="Non acute Inpatient with Inpatient">

    val argmapFornonacuteExclusion = mutable.Map(KpiConstants.eligibleDfName -> inPatientstayDf, KpiConstants.refHedisTblName -> refHedisDf,
                                                 KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)
    val nonAcuteInPatValList = List(KpiConstants.nonacuteInPatStayVal)
    val nonAcuteInPatCodeSystem = List(KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val nonAcuteInPatDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapFornonacuteExclusion, nonAcuteInPatValList, nonAcuteInPatCodeSystem)
                                              .select(KpiConstants.memberidColName)

    //</editor-fold>


    //</editor-fold>

    val optionalExclDf = 	esrdobsKidneyTrDf.union(pregnancyDf).union(nonAcuteInPatDf)
    optionalExclDf.cache()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="Numerator Calculation for Non Supplemental Data ">


    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val eligNumsuppNDf = inputForOptExclAndNumDf.filter(($"${KpiConstants.supplflagColName}".===("N"))
                                                     && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList)))
    val argmapNumSuppNdata = mutable.Map(KpiConstants.eligibleDfName -> eligNumsuppNDf, KpiConstants.refHedisTblName -> refHedisDf,
                                         KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)


    //<editor-fold desc=" Outpatient Without UBREV,Nonacute Inpatient,Remote Blood Pressure Monitoring valueset">

    val outpatnonacuterebpValList = List(KpiConstants.outpatwoUbrevVal, KpiConstants.nonAcuteInPatientVal,KpiConstants.remotebpmVal)
    val outpatnonacuterebpCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinForoutpatnonacuterebpnsuppDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumSuppNdata,outpatnonacuterebpValList, outpatnonacuterebpCodeSystem)
    val outpatnonacuterebpnsuppDf = UtilFunctions.measurementYearFilter(joinForoutpatnonacuterebpnsuppDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)

    //</editor-fold>

    //<editor-fold desc="bpReading (Systolic Less Than 140)">

    val argmapNumSuppNsysdata = mutable.Map(KpiConstants.eligibleDfName -> outpatnonacuterebpnsuppDf, KpiConstants.refHedisTblName -> refHedisDf,
                                            KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val sysbpReadingValList = List(KpiConstants.systolicLt140Val)
    val sysbpReadingCodeSystem = List(KpiConstants.cptCatIIVal)
    val sysbpReadingnsuppDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumSuppNsysdata, sysbpReadingValList, sysbpReadingCodeSystem)

    //</editor-fold>

    //<editor-fold desc="Diastolic 80-90 and less than 80">

    val argmapNumSuppNdiasdata = mutable.Map(KpiConstants.eligibleDfName -> sysbpReadingnsuppDf, KpiConstants.refHedisTblName -> refHedisDf,
                                            KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)
    val diasValList = List(KpiConstants.diastolicLt80Val, KpiConstants.diastolicBtwn8090Val)
    val diasCodeSystem = List(KpiConstants.cptCatIIVal)
    val joinFordiasbpReadingDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumSuppNdiasdata, diasValList, diasCodeSystem)
    val numeratorNsnDf = joinFordiasbpReadingDf.groupBy(KpiConstants.memberidColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                           first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                                .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                                .select(KpiConstants.memberidColName)

    //</editor-fold>

    //val numeratorDf = outpatnonacuterebpDf.intersect(sysbpReadingDf).intersect(diasbpReadingDf)
    numeratorNsnDf.cache()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation for other data">

    val eligNumsuppDf = inputForOptExclAndNumDf.as("df1").join(numeratorNsnDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                                                .filter(($"df2.${KpiConstants.memberidColName}".isNull) && ($"df1.${KpiConstants.claimstatusColName}".isin(claimStatusList)))

    val argmapNumSuppdata = mutable.Map(KpiConstants.eligibleDfName -> eligNumsuppDf, KpiConstants.refHedisTblName -> refHedisDf,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)



    //<editor-fold desc=" Outpatient Without UBREV,Nonacute Inpatient,Remote Blood Pressure Monitoring valueset">

    val joinForoutpatnonacuterebpDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumSuppNdata,outpatnonacuterebpValList, outpatnonacuterebpCodeSystem)
    val outpatnonacuterebpDf = UtilFunctions.measurementYearFilter(joinForoutpatnonacuterebpDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)

    //</editor-fold>

    //<editor-fold desc="bpReading (Systolic Less Than 140)">

    val argmapNumsysdata = mutable.Map(KpiConstants.eligibleDfName -> outpatnonacuterebpDf, KpiConstants.refHedisTblName -> refHedisDf,
                                       KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)
    val sysbpReadingDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumsysdata, sysbpReadingValList, sysbpReadingCodeSystem)

    //</editor-fold>

    //<editor-fold desc="Diastolic 80-90 and less than 80">

    val argmapNumdiasdata = mutable.Map(KpiConstants.eligibleDfName -> sysbpReadingDf, KpiConstants.refHedisTblName -> refHedisDf,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)
    val joinFordiasbpReadingsDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumdiasdata, diasValList, diasCodeSystem)
    val numeratorsDf = joinFordiasbpReadingsDf.groupBy(KpiConstants.memberidColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                         first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                              .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                              .select(KpiConstants.memberidColName)

    //</editor-fold>

    numeratorsDf.cache()
    //</editor-fold>

    val numeratorDf = numeratorNsnDf.union(numeratorsDf)
    //</editor-fold>

    //<editor-fold desc="Ncqa Output Creation Code">

    val dfMapForNcqaOut = mutable.Map(KpiConstants.totalPopDfName -> totalPopulationDf , KpiConstants.eligibleDfName -> eligibleDf.select(KpiConstants.memberidColName),
                                      KpiConstants.mandatoryExclDfname -> mandatoryExclDf , KpiConstants.optionalExclDfName -> optionalExclDf,
                                      KpiConstants.numeratorDfName -> numeratorNsnDf)
    //</editor-fold>





    /*commented numerator code*/
   /* val outpatnonacuterebpValList = List(KpiConstants.outpatwoUbrevVal, KpiConstants.nonAcuteInPatientVal,KpiConstants.remotebpmVal)
    val outpatnonacuterebpCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal)
    val joinForoutpatnonacuterebpDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapForNumSuppNDf,outpatnonacuterebpValList, outpatnonacuterebpCodeSystem)
    val outpatnonacuterebpDf = UtilFunctions.measurementYearFilter(joinForoutpatnonacuterebpDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement0Val)
      .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))
      .groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
      first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
      .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
      .select(KpiConstants.memberskColName)




      val sysbpReadingValList = List(KpiConstants.systolicLt140Val)
    val sysbpReadingCodeSystem = List(KpiConstants.cptCatIIVal)
    val joinForsysbpReadingDf = UtilFunctions.joinWithRefHedisFunction(spark, argmapNumSuppNsysdata, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.cbpMeasureId, sysbpReadingValList, sysbpReadingCodeSystem)
    val sysbpReadingDf = UtilFunctions.measurementYearFilter(joinForsysbpReadingDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                            .filter($"${KpiConstants.claimstatusColName}".isin(claimStatusList))
                                            .groupBy(KpiConstants.memberskColName).agg(max($"${KpiConstants.serviceDateColName}").alias(KpiConstants.maxserviceDateColName),
                                                                                       first($"${KpiConstants.secondDiagColName}").alias(KpiConstants.secondDiagColName))
                                            .filter($"${KpiConstants.maxserviceDateColName}".>=($"${KpiConstants.secondDiagColName}"))
                                            .select(KpiConstants.memberskColName)



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
