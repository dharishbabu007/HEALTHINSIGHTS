package com.itc.ncqa.main

import com.itc.ncqa.Constants
import com.itc.ncqa.Constants.KpiConstants
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.year
//import com.itc.ncqa.Functions.SparkObject.spark
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
    val conf = new SparkConf().setAppName("NcqaProgram")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
      /*.set("spark.executor.memory", "5g")
      .set("spark.driver.memory", "5g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size","16g")*/

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._


    val aLiat = List("col1")

    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))
                                        .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                        .withColumn(KpiConstants.memStartDateColName, to_date($"${KpiConstants.memStartDateColName}", KpiConstants.dateFormatString))
                                        .withColumn(KpiConstants.memEndDateColName, to_date($"${KpiConstants.memEndDateColName}", KpiConstants.dateFormatString))
                                        .withColumn(KpiConstants.dateofbirthColName, to_date($"${KpiConstants.dateofbirthColName}", KpiConstants.dateFormatString))



    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val visitsDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,aLiat)
                                   .filter(($"${KpiConstants.serviceDateColName}".isNotNull)
                                       && (($"${KpiConstants.admitDateColName}".isNotNull && $"${KpiConstants.dischargeDateColName}".isNotNull)
                                            || ($"${KpiConstants.admitDateColName}".isNull && $"${KpiConstants.dischargeDateColName}".isNull))
                                       && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
                                   .drop(KpiConstants.lobProductColName, "latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name","product")
                                   .withColumn(KpiConstants.serviceDateColName, to_date($"${KpiConstants.serviceDateColName}", KpiConstants.dateFormatString))
                                   .withColumn(KpiConstants.admitDateColName, when($"${KpiConstants.admitDateColName}".isNotNull,to_date($"${KpiConstants.admitDateColName}",  KpiConstants.dateFormatString)))
                                   .withColumn(KpiConstants.dischargeDateColName, when($"${KpiConstants.dischargeDateColName}".isNotNull,to_date($"${KpiConstants.dischargeDateColName}",  KpiConstants.dateFormatString)))
                                   .withColumn(KpiConstants.medstartdateColName, when($"${KpiConstants.medstartdateColName}".isNotNull,to_date($"${KpiConstants.medstartdateColName}",  KpiConstants.dateFormatString)))



    val medmonmemDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,aLiat)
                                       .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")

    val productPlanDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.productPlanTblName,aLiat)
                                         .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")



    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter(($"${KpiConstants.measureIdColName}".===(KpiConstants.cbpMeasureId)) || ($"${KpiConstants.measureIdColName}".===(KpiConstants.ggMeasureId)))
                                      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                      .cache()
    refHedisDf.count()
    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.cbpMeasureId))
                                             .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                             .cache()
    ref_medvaluesetDf.count()

    //</editor-fold

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Age Filter">

    val ageFilterDf = UtilFunctions.ageFilter(membershipDf, KpiConstants.dateofbirthColName, year, KpiConstants.age18Val, KpiConstants.age86Val, KpiConstants.boolTrueVal, KpiConstants.boolTrueVal)

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))

    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName, lit(contEnrollStartDate).cast(DateType))
      .withColumn(KpiConstants.contenrollUppCoName, lit(contEnrollEndDate).cast(DateType))
      .withColumn(KpiConstants.anchorDateColName, lit(contEnrollEndDate).cast(DateType))


    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      ||($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}") && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}"))))
      .withColumn(KpiConstants.anchorflagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}")), lit(1)).otherwise(lit(0)))
      .withColumn(KpiConstants.contEdFlagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}")), lit(1)).otherwise(lit(0)))



    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    val listDf = contEnrollStep1Df.groupBy($"${KpiConstants.memberidColName}")
      .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
        sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName))
      .filter((date_add($"max_mem_end_date",KpiConstants.days45).>=($"${KpiConstants.contenrollUppCoName}"))
        && (date_sub($"min_mem_start_date",KpiConstants.days45).<=($"${KpiConstants.contenrollLowCoName}"))
        &&($"${KpiConstants.anchorflagColName}").>(0))
      .select($"${KpiConstants.memberidColName}")

    val contEnrollStep2Df = contEnrollStep1Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select("df1.*")


    // contEnrollStep3Df.printSchema()
    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberidColName}").orderBy(org.apache.spark.sql.functions.col(s"${KpiConstants.memEndDateColName}").desc,org.apache.spark.sql.functions.col(s"${KpiConstants.memStartDateColName}"))


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep3Df = contEnrollStep2Df.withColumn(KpiConstants.overlapFlagColName, when(($"${KpiConstants.memStartDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memStartDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))
      && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal))))
      ,lit(1))
      .when(($"${KpiConstants.memStartDateColName}".<(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal)))
        && ($"${KpiConstants.memEndDateColName}".>=(lag($"${KpiConstants.memStartDateColName}",1 ).over(contWindowVal)) && $"${KpiConstants.memEndDateColName}".<=(lag($"${KpiConstants.memEndDateColName}",1).over(contWindowVal)))
        ,lit(2)).otherwise(lit(0)))
      .withColumn(KpiConstants.coverageDaysColName,when($"${KpiConstants.overlapFlagColName}".===(0) ,datediff(when($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"), $"${KpiConstants.memEndDateColName}").otherwise($"${KpiConstants.contenrollUppCoName}")
        ,when($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}"), $"${KpiConstants.memStartDateColName}").otherwise($"${KpiConstants.contenrollLowCoName}"))+ 1 )
        .when($"${KpiConstants.overlapFlagColName}".===(2), datediff( when($"${KpiConstants.contenrollLowCoName}".<=(lag( $"${KpiConstants.contenrollUppCoName}",1).over(contWindowVal)), $"${KpiConstants.memEndDateColName}").otherwise(lag( $"${KpiConstants.contenrollUppCoName}",1).over(contWindowVal))
          ,$"${KpiConstants.memStartDateColName}")+1 )
        .otherwise(0))
      .withColumn(KpiConstants.countColName, when(when($"${KpiConstants.overlapFlagColName}".===(0), datediff(lag($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
        .otherwise(0).>(1),lit(1))
        .otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep3Df.groupBy(KpiConstants.memberidColName)
      .agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
        sum($"${KpiConstants.coverageDaysColName}").alias(KpiConstants.coverageDaysColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))



    val contEnrollmemDf = contEnrollStep5Df.filter(((($"${KpiConstants.countColName}") + (when(date_sub($"min_mem_start_date", 1).>=($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
      + (when(date_add($"max_mem_end_date", 1).<=($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
      && ($"${KpiConstants.coverageDaysColName}".>=(320)))
      .select(KpiConstants.memberidColName).distinct()



    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}")


    contEnrollDf.select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName)
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/contEnrolldetail/")

    //</editor-fold>





    /*

    //<editor-fold desc="Hospice Removal">

    val argmapforHospice = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val visitSchema = visitsDf.schema
    val hospiceValList = List(KpiConstants.hospiceVal)
    val hospiceCodeSystem = KpiConstants.codeSystemList
    val hospiceClaimsDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforHospice,hospiceValList,hospiceCodeSystem)
    val hospiceincurryearDf = UtilFunctions.measurementYearFilter(hospiceClaimsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                           .select(KpiConstants.memberidColName).distinct()

 /*   hospiceincurryearDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/hospiceincurryearDf/")*/
    val contEnrollMemDf = contEnrollDf.select(KpiConstants.memberidColName).distinct()
    val hosremMemidDf = contEnrollMemDf.except(hospiceincurryearDf)

    val hospiceRemovedMemsDf  = contEnrollDf.as("df1").join(hosremMemidDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                             .select("df1.*").cache()

    hospiceRemovedMemsDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/hospiceRemovedMemsDf/")

    //</editor-fold>

    //<editor-fold desc="Eligible Event calculation">

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> hospiceRemovedMemsDf.select(KpiConstants.memberidColName).distinct(), KpiConstants.visitTblName -> visitsDf)
    val visitJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).cache()
    visitJoinedDf.count()
    //</editor-fold>


    val visitNonSupplDf = visitJoinedDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    val baseargMap = mutable.Map(KpiConstants.eligibleDfName -> visitNonSupplDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                 KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Outpatient without UBREV ">

    val outPatwoUbrevValList = List(KpiConstants.outpatwoUbrevVal)
    val outPatwoUbrevCodeSystem = KpiConstants.codeSystemList
    val joinForOutpatWoUbrevDf = UtilFunctions.joinWithRefHedisFunction(spark, baseargMap,outPatwoUbrevValList,outPatwoUbrevCodeSystem)
    val outpatientWoUbrevDf = UtilFunctions.measurementYearFilter(joinForOutpatWoUbrevDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)

   /* outpatientWoUbrevDf.select(KpiConstants.memberidColName,KpiConstants.serviceDateColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/outpatientWoUbrev/")*/

    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension">

    val outpatessargMap= mutable.Map(KpiConstants.eligibleDfName -> outpatientWoUbrevDf , KpiConstants.refHedisTblName ->refHedisDf)
    val essHypTenValList = List(KpiConstants.essentialHyptenVal)
    val essCodeSsytem = KpiConstants.codeSystemList
    val outpatwessentialHyptensDf = UtilFunctions.joinWithRefHedisFunction(spark, outpatessargMap,  essHypTenValList, essCodeSsytem)

  /*  outpatwessentialHyptensDf.select(KpiConstants.memberidColName,KpiConstants.serviceDateColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/outpatwessentialHyptens/")*/

    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension with TeleHealth Modifier">

    val outpatesswtelhealthargMap = mutable.Map(KpiConstants.eligibleDfName -> outpatwessentialHyptensDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                                KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val teleHealthModValList = List(KpiConstants.telehealthModifierVal)
    val teleHealthModCodeSystem = KpiConstants.codeSystemList
    val optesshyptentelheaModDf =  UtilFunctions.joinWithRefHedisFunction(spark, outpatesswtelhealthargMap,teleHealthModValList,teleHealthModCodeSystem)
                                                .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName).cache()

   /* optesshyptentelheaModDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/optesshyptentelheaModDf/")*/

    //</editor-fold>

    //<editor-fold desc="Outpatient without UBREV with Essential Hyper Tension without Telehealth Modifier">

    val opesshyptenwotelheaDf = (outpatwessentialHyptensDf.select(KpiConstants.memberidColName,KpiConstants.serviceDateColName)).except(optesshyptentelheaModDf.withColumnRenamed(KpiConstants.memberidColName, "member_id1").withColumnRenamed(KpiConstants.serviceDateColName,"service_date1"))
      .cache()

   /* opesshyptenwotelheaDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/opesshyptenwotelheaDf/")*/
    //</editor-fold>

    //<editor-fold desc="Telephone visits">

    val telephoneVistValList = List(KpiConstants.telephoneVisitsVal)
    val telephoneVistCodeSystem = KpiConstants.codeSystemList
    val joinFortelephoneVistDf = UtilFunctions.joinWithRefHedisFunction(spark, baseargMap, telephoneVistValList,telephoneVistCodeSystem)
    val telephoneVistDf = UtilFunctions.measurementYearFilter(joinFortelephoneVistDf, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)

    //</editor-fold>

    //<editor-fold desc="Telephone visits with Essential Hypertension">

    val telephoneessargMap= mutable.Map(KpiConstants.eligibleDfName -> telephoneVistDf , KpiConstants.refHedisTblName ->refHedisDf ,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val telephoneEssentialHypDf = UtilFunctions.joinWithRefHedisFunction(spark, telephoneessargMap, essHypTenValList,essCodeSsytem)
                                               .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName).cache()

   /* telephoneEssentialHypDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/telephoneEssentialHypDf/")*/
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
                                               .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName).cache()

  /*  onlineEssentialHypDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/onlineEssentialHypDf/")*/
    //</editor-fold>


    //<editor-fold desc="Event1">


    val event1Df = opesshyptenwotelheaDf.select(s"${KpiConstants.memberidColName}", s"${KpiConstants.serviceDateColName}").as("df1").join(telephoneEssentialHypDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date1").as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                         .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.service_date1"))
                                                         .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.service_date1"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                                    .otherwise($"df2.service_date1"))
                                                         .select(s"df1.${KpiConstants.memberidColName}",KpiConstants.secondDiagColName)


    /*event1Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/event1Df/")*/

    //</editor-fold>

    //<editor-fold desc="Event2">

    val event2df = opesshyptenwotelheaDf.as("df1").join(opesshyptenwotelheaDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date1").as("df2"), KpiConstants.memberidColName)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.service_date1"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.service_date1"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.service_date1"))
                                              .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)

   /* event2df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/event2df/")*/

    //</editor-fold>

    //<editor-fold desc="Event3">

    val event3Df  = opesshyptenwotelheaDf.as("df1").join(optesshyptentelheaModDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date1").as("df2"), KpiConstants.memberidColName)
                                               .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.service_date1"))
                                               .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.service_date1"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.service_date1"))
                                               .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)


   /* event3Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/event3Df/")*/
    //</editor-fold>

    //<editor-fold desc="Event4">

    val event4Df = opesshyptenwotelheaDf.as("df1").join(onlineEssentialHypDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date1").as("df2"), KpiConstants.memberidColName)
                                              .filter($"df1.${KpiConstants.serviceDateColName}".=!=($"df2.service_date1"))
                                              .withColumn(KpiConstants.secondDiagColName, when($"df1.${KpiConstants.serviceDateColName}".>=($"df2.service_date1"), $"df1.${KpiConstants.serviceDateColName}")
                                                                                          .otherwise($"df2.service_date1"))
                                              .select(KpiConstants.memberidColName,KpiConstants.secondDiagColName)


   /* event4Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/event4Df/")*/
    //</editor-fold>

    val eventInDf = event1Df.union(event2df).union(event3Df).union(event4Df).dropDuplicates()

    val eventDf = eventInDf.groupBy(KpiConstants.memberidColName).agg(min(KpiConstants.secondDiagColName).alias(KpiConstants.secondDiagColName))

  /*  eventDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/eventDf/")*/
    val totalPopulationClaimsDf = visitJoinedDf.as("df1").join(eventDf.as("df2"), KpiConstants.memberidColName)
                                                         .select(s"df1.*", s"df2.${KpiConstants.secondDiagColName}")


    val inputForMandCalDf = totalPopulationClaimsDf.as("df1").join(hospiceRemovedMemsDf.as("df2"), KpiConstants.memberidColName)
                                                                    .select("df1.*",s"df2.${KpiConstants.lobColName}", s"df2.${KpiConstants.lobProductColName}").cache()

    val totalPopMemIdDf = totalPopulationClaimsDf.select(KpiConstants.memberidColName).distinct()
    val totalpopOutDf = totalPopMemIdDf.as("df1").join(hospiceRemovedMemsDf.as("df2"), KpiConstants.memberidColName)
                                                        .select(s"df1.${KpiConstants.memberidColName}", s"df2.${KpiConstants.lobProductColName}", s"df2.${KpiConstants.payerColName}")
                                                        .cache()

    /*totalpopOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/totalpopOut/")*/

    inputForMandCalDf.count()
    visitJoinedDf.unpersist()
    visitNonSupplDf.unpersist()
    hospiceRemovedMemsDf.unpersist()

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

   // val inputForMandExclDf = totalPopMemIdDf
    val argmapForMandExcl = mutable.Map(KpiConstants.eligibleDfName -> inputForMandCalDf, KpiConstants.refHedisTblName -> refHedisDf,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Mandatory Exclusion1(Only for Medicare)">

    val ageCheckDate = year + "12-31"
    val mandatoryExcl1Df = inputForMandCalDf.as("df1").join(medmonmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                        .filter(($"df1.${KpiConstants.lobColName}".===(KpiConstants.medicareLobName)) && (add_months($"df1.${KpiConstants.dobColName}",KpiConstants.months792).<=(ageCheckDate))
                                                            && ((($"df2.${KpiConstants.ltiFlagColName}".===(KpiConstants.boolTrueVal))) || ($"df1.${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))))
                                                        .select(s"df1.${KpiConstants.memberidColName}").cache()

   /* mandatoryExcl1Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/mandatoryExcl1Df/")*/
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion2">

    val mandatoryExcl2Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age81Val, KpiConstants.age120Val)

  /*  mandatoryExcl2Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/mandatoryExcl2Df/")*/

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion3">

    val fralityAndAgeGt66Df = UtilFunctions.findFralityMembers(spark,argmapForMandExcl,year,KpiConstants.age66Val, KpiConstants.age120Val)
                                           .select(KpiConstants.memberidColName)



    //<editor-fold desc="Advanced Illness valueset">

    val advillValList = List(KpiConstants.advancedIllVal)
    val advillCodeSystem = KpiConstants.codeSystemList
    val joinedForAdvancedIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapForMandExcl,advillValList,advillCodeSystem)
    val advancedIllDf = UtilFunctions.measurementYearFilter(joinedForAdvancedIllDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val,KpiConstants.measurement1Val)

    //</editor-fold>

    val argMapwithadvill = mutable.Map(KpiConstants.eligibleDfName -> advancedIllDf, KpiConstants.refHedisTblName -> refHedisDf,
                                      KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    //<editor-fold desc="Outpatient Valueset with Advanced Illness">

    val outPatValList = List(KpiConstants.outPatientVal)
    val outPatCodeSystem = KpiConstants.codeSystemList
    val outpatAndAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,outPatValList,outPatCodeSystem)
                                         .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Observation Valueset with Advanced Illness">

    val obsVisitValList = List(KpiConstants.observationVal)
    val obsVisitCodeSystem = KpiConstants.codeSystemList
    val observationAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,obsVisitValList,obsVisitCodeSystem)
                                     .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Ed Valueset with Advanced Illness">

    val edVisitValList = List(KpiConstants.edVal)
    val edVisitCodeSystem = KpiConstants.codeSystemList
    val edVisitsAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,edVisitValList,edVisitCodeSystem)
                                        .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Non Acutr InPatient Valueset with Advanced Illness">

    val nonAcuteInValList = List(KpiConstants.nonAcuteInPatientVal)
    val nonAcuteInCodeSsytem = KpiConstants.codeSystemList
    val nonAcuteInPatAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,nonAcuteInValList,nonAcuteInCodeSsytem)
                                             .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //</editor-fold>

    //<editor-fold desc="Acute InPatient Valueset with Advanced Illness">

    val acuteInPatValLiat = List(KpiConstants.accuteInpatVal)
    val acuteInPatCodeSystem = KpiConstants.codeSystemList
    val acuteInpatAdvIllDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapwithadvill,acuteInPatValLiat,acuteInPatCodeSystem)
                                          .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="Dementia Medication List">

    val dementiaValList = List(KpiConstants.dementiaVal)
    val dementiaCodeSystem = KpiConstants.codeSystemList
    val inputForMandCalschema = inputForMandCalDf.schema
    val joinedForDemMed1Df = UtilFunctions.joinWithRefHedisFunction(spark,argmapForMandExcl,dementiaValList,dementiaCodeSystem)
    val dementiaMed1Df = if(joinedForDemMed1Df.count()> 0) { UtilFunctions.mesurementYearFilter(joinedForDemMed1Df, KpiConstants.serviceDateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                                                          .select(KpiConstants.memberidColName)}else {spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputForMandCalschema)}


    val dementiaMedValList = List(KpiConstants.dementiaMedicationVal)
    val joinedForDemMed2Df = UtilFunctions.joinWithRefMedFunction(spark,argmapForMandExcl,dementiaMedValList)
    val dementiaMed2Df = if(joinedForDemMed1Df.count()> 0) { UtilFunctions.mesurementYearFilter(joinedForDemMed2Df, KpiConstants.medstartdateColName, year, KpiConstants.measurement0Val, KpiConstants.measurement1Val)
                                                                          .select(KpiConstants.memberidColName)}else {spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputForMandCalschema)}

    val dementiaMedDf = dementiaMed1Df.select(KpiConstants.memberidColName).union(dementiaMed2Df.select(KpiConstants.memberidColName))
    //</editor-fold>



    val mandatoryExcl3_2Df = outpatAndAdvIllDf.union(observationAdvIllDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date1"))
                                              .union(edVisitsAdvIllDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date2"))
                                              .union(nonAcuteInPatAdvIllDf.withColumnRenamed(KpiConstants.serviceDateColName, "service_date3"))
                                              .groupBy(KpiConstants.memberidColName)
                                              .agg(countDistinct($"${KpiConstants.serviceDateColName}").alias(KpiConstants.countColName))
                                              .filter($"${KpiConstants.countColName}".>=(KpiConstants.count2Val))
                                              .select(KpiConstants.memberidColName)


    val mandatoryExcl3Df = fralityAndAgeGt66Df.intersect((mandatoryExcl3_2Df.union(acuteInpatAdvIllDf).union(dementiaMedDf)))

    mandatoryExcl3Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/mandatoryExcl3Df/")

    //</editor-fold>


    val mandatoryExclDf = mandatoryExcl1Df.union(mandatoryExcl2Df).union(mandatoryExcl3Df).dropDuplicates()

    mandatoryExclDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/mandatoryExclDf/")

    //</editor-fold>

     val eligiblePopDf = totalPopMemIdDf.except(mandatoryExclDf.select(KpiConstants.memberidColName))
     val eligibleClaimsDf =  inputForMandCalDf.as("df1").join(eligiblePopDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                         .select("df1.*").cache()

    eligibleClaimsDf.count()
    inputForMandCalDf.unpersist()
    eligiblePopDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/eligiblePop/")

    //</editor-fold>

    //<editor-fold desc="Denominator Calculation">

    val denominatorDf = eligiblePopDf
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val inputForOptExclAndNumDf = eligibleClaimsDf

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

    val optionalExclDf = 	esrdobsKidneyTrDf.union(pregnancyDf).union(nonAcuteInPatDf).dropDuplicates()
    optionalExclDf.cache()
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="Numerator Calculation for Non Supplemental Data ">


    val eligNumsuppNDf = inputForOptExclAndNumDf.filter($"${KpiConstants.supplflagColName}".===("N"))
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

*/


    spark.sparkContext.stop()

  }

}
