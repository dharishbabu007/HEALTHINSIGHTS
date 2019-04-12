package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable


object NcqaCBP {

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Reading program arguments and SaprkSession oBject creation">

    val year = args(0)
    val measureId = args(1)
    val dbName = args(2)
    val baseMsrPath = args(3)

    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)
    val conf = new SparkConf().setAppName("NcqaProgram")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")


    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val jobId = spark.sparkContext.applicationId
    val baseDir = baseMsrPath + "/" + jobId
    val outDir = baseDir + "/Out"
    val intermediateDir = baseDir + "/Intermediate"

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val ystDate = year+"-01-01"
    val yendDate = year+"-12-31"
    val aLiat = List("col1")
    val msrVal = s"'$measureId'"
    val ggMsrId = s"'${KpiConstants.ggMeasureId}'"
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName,KpiConstants.medicareLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
    val memqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.membershipTblName} WHERE measure = $msrVal AND (member_plan_start_date IS  NOT NULL) AND(member_plan_end_date IS NOT NULL)"""
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,memqueryString,aLiat)
                                        .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(2)





    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val visitqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.visitTblName} WHERE measure = $msrVal and  (service_date IS  NOT NULL) AND((admit_date IS NULL and discharge_date IS NULL) OR (ADMIT_DATE IS NOT NULL AND DISCHARGE_DATE IS NOT NULL))"""
    val visitsDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,visitqueryString,aLiat)
                                    .filter((($"${KpiConstants.dataSourceColName}".===("Claim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
                                           ||(($"${KpiConstants.dataSourceColName}".===("RxClaim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
                                           ||($"${KpiConstants.dataSourceColName}".===("Rx"))
                                           || ($"${KpiConstants.dataSourceColName}".===("Lab")))
                                    .drop(KpiConstants.lobProductColName, "latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name","product")
                                    .withColumn(KpiConstants.revenuecodeColName, when((length($"${KpiConstants.revenuecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.revenuecodeColName}"))).otherwise($"${KpiConstants.revenuecodeColName}"))
                                    .withColumn(KpiConstants.billtypecodeColName, when((length($"${KpiConstants.billtypecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.billtypecodeColName}"))).otherwise($"${KpiConstants.billtypecodeColName}"))
                                    .withColumn(KpiConstants.proccode2ColName, when(($"${KpiConstants.proccode2mod1ColName}".isin(KpiConstants.avoidCodeList:_*)) || ($"${KpiConstants.proccode2mod2ColName}".isin(KpiConstants.avoidCodeList:_*)),lit("NA")).otherwise($"${KpiConstants.proccode2ColName}"))
      .repartition(2)

    val medmonmemqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.medmonmemTblName} WHERE measure = $msrVal"""
    val medmonmemDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,medmonmemqueryString,aLiat)
      .filter(($"run_date".>=(ystDate)) && ($"run_date".<=(yendDate)))
                                        .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(2)

    val refHedisqueryString =s"""SELECT * FROM  ${KpiConstants.dbName}.${KpiConstants.refHedisTblName} WHERE measureid = $msrVal OR measureid= $ggMsrId"""
    val refHedisDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,refHedisqueryString,aLiat)
   /* val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter(($"${KpiConstants.measureIdColName}".===(KpiConstants.cbpMeasureId))
                                           || ($"${KpiConstants.measureIdColName}".===(KpiConstants.ggMeasureId)))*/
                                      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


    val refMedqueryString = s"""SELECT * FROM  ${KpiConstants.dbName}.${KpiConstants.refmedValueSetTblName} WHERE measure_id = $msrVal"""
    val ref_medvaluesetDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,refMedqueryString,aLiat)
   /* val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.cbpMeasureId))*/
                                             .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


   // println("counts:"+membershipDf.count()+","+ visitsDf.count()+","+medmonmemDf.count() +","+refHedisDf.count()+","+ref_medvaluesetDf.count())

    //</editor-fold

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Age Filter">


    val ageChkDate = year+"-12-31"
    val ageFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months216).<=(ageChkDate))
                                  && (UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months1032).>(ageChkDate)))

    val sn2Members = ageFilterDf.filter(($"${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))
                                      &&(((($"${KpiConstants.memStartDateColName}".>=(ystDate)) && ($"${KpiConstants.memStartDateColName}".<=(yendDate)))
                                        ||(($"${KpiConstants.memEndDateColName}".>=(ystDate)) && ($"${KpiConstants.memEndDateColName}".<=(yendDate))))
                                        || ((($"${KpiConstants.memStartDateColName}".<(ystDate)) && ($"${KpiConstants.memEndDateColName}".>(yendDate)))))
                                      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months792).<=(ageChkDate)))
                                .select(KpiConstants.memberidColName).rdd.map(r => r.getString(0)).collect()
    val sn2RemovedMemDf = ageFilterDf.except(ageFilterDf.filter($"${KpiConstants.memberidColName}".isin(sn2Members:_*)))

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = sn2RemovedMemDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname, KpiConstants.memStartDateColName,
      KpiConstants.memEndDateColName, KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName,
      KpiConstants.primaryPlanFlagColName)

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
        ,when($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}"), $"${KpiConstants.memStartDateColName}").otherwise($"${KpiConstants.contenrollLowCoName}"))+ 1 )
        .when($"${KpiConstants.overlapFlagColName}".===(2), datediff( when($"${KpiConstants.contenrollLowCoName}".>=(lag( $"${KpiConstants.memStartDateColName}",1).over(contWindowVal)), $"${KpiConstants.contenrollLowCoName}").otherwise(lag( $"${KpiConstants.memStartDateColName}",1).over(contWindowVal))
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
      .withColumn(KpiConstants.reqCovDaysColName, (datediff($"${KpiConstants.contenrollUppCoName}", $"${KpiConstants.contenrollLowCoName}")-44))



    val contEnrollmemDf = contEnrollStep5Df.filter(((($"${KpiConstants.countColName}") + (when(date_sub($"min_mem_start_date", 1).>=($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
      + (when(date_add($"max_mem_end_date", 1).<=($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
      && ($"${KpiConstants.coverageDaysColName}".>=($"${KpiConstants.reqCovDaysColName}")))
      .select(KpiConstants.memberidColName).distinct()


    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}",s"df1.${KpiConstants.primaryPlanFlagColName}")
      .repartition(2).cache()

    contEnrollDf.count()


    //</editor-fold

    //<editor-fold desc="Dual eligibility,Dual enrollment, Ima enrollment filter">

    val baseOutInDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList,measureId)
    val medicareContEnrollDf = baseOutInDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))

    val medicareHospiceDf = medicareContEnrollDf.as("df1").join(medmonmemDf.as("df2"), Seq(KpiConstants.memberidColName))
      .groupBy(KpiConstants.memberidColName).agg(countDistinct(when($"${KpiConstants.hospiceFlagColName}".===(KpiConstants.yesVal),1)).alias(KpiConstants.countColName))
      .filter($"${KpiConstants.countColName}".>(0))
      .select(KpiConstants.memberidColName).rdd.map(r=> r.getString(0)).collect()

    val baseOutDf = baseOutInDf.except(baseOutInDf.filter($"${KpiConstants.memberidColName}".isin(medicareHospiceDf:_*)))

    baseOutDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/baseOutDf/")

    //</editor-fold>

    //<editor-fold desc="Initial Join with Ref_Hedis">

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf,
      KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.outpatwoUbrevVal, KpiConstants.telehealthModifierVal,
      KpiConstants.essentialHyptenVal, KpiConstants.telephoneVisitsVal, KpiConstants.onlineAssesmentVal, KpiConstants.fralityVal,
      KpiConstants.outPatientVal, KpiConstants.advancedIllVal, KpiConstants.observationVal, KpiConstants.edVal, KpiConstants.nonAcuteInPatientVal,
      KpiConstants.acuteInpatientVal, KpiConstants.remotebpmVal, KpiConstants.systolicLt140Val, KpiConstants.diastolicBtwn8090Val,
      KpiConstants.diastolicLt80Val, KpiConstants.esrdVal, KpiConstants.esrdObsoleteVal,KpiConstants.kidneyTransplantVal,
      KpiConstants.pregnancyVal, KpiConstants.inpatientStayVal, KpiConstants.nonacuteInPatStayVal, KpiConstants.systolicGtOrEq140Val, KpiConstants.diastolicGt90Val)


    val medicationlList = List(KpiConstants.dementiaMedicationVal)
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medicationlList)
    //visitRefHedisDf.show()
    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
                                        .select(KpiConstants.memberidColName, KpiConstants.dobColName, KpiConstants.genderColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
                                                KpiConstants.supplflagColName, KpiConstants.valuesetColName)
    //visitgroupedDf.show()

    val indLabVisRemDf = visitgroupedDf.filter((!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)))
                                       .repartition(2).cache()
    indLabVisRemDf.count()

    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val baseMemDf = spark.read.parquet(intermediateDir+ "/baseOutDf/").repartition(2)
    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate)
      && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .dropDuplicates()
      .rdd
      .map(r=> r.getString(0))
      .collect()


    val hospiceRemMemEnrollDf = baseMemDf.except(baseMemDf.filter($"${KpiConstants.memberidColName}".isin(hospiceInCurrYearMemDf:_*)))

    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> hospiceRemMemEnrollDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val validVisitsOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).repartition(2).cache()
    validVisitsOutDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/validVisitsOutDf/")

    //</editor-fold>

    //<editor-fold desc="Eligible Event calculation">

    val validVisitsDf = spark.read.parquet(intermediateDir+ "/validVisitsOutDf/").repartition(2).cache()
    validVisitsDf.count()
    baseOutDf.unpersist()
    indLabVisRemDf.unpersist()

    val visitForEventDf = validVisitsDf.filter($"${KpiConstants.supplflagColName}".===("N"))


    //<editor-fold desc="Event Calculation">

    val eventStartDate = year.toInt -1 +"-01-01"
    val eventEndDate = year+"-12-31"

    /*Find out the memebers who has Outpatient without UBREV, Essential HyperTension and not Telehealth Modifier*/
    val eventStep1Df = visitForEventDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.essentialHyptenVal))
      && (!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.telehealthModifierVal))
      &&($"${KpiConstants.serviceDateColName}".>=(eventStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(eventEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    //<editor-fold desc="First event Calculation with one eventStep1 VISITS">

    /*Find out the single visit with the condtion*/
    val oneVisitEventDf = eventStep1Df.groupBy(KpiConstants.memberidColName)
      .agg(countDistinct(KpiConstants.serviceDateColName).alias("count"),
        first($"${KpiConstants.serviceDateColName}").alias(KpiConstants.serviceDateColName))
      .filter($"count".===(1))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    /*Find out the memebers who has Outpatient,Essential Hypertension,and(Telehealth or Telephone )*/
    val eventStep2Df = visitForEventDf.filter( (((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.telehealthModifierVal)))
      ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.onlineAssesmentVal))
      ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.telephoneVisitsVal)))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.essentialHyptenVal))
      &&($"${KpiConstants.serviceDateColName}".>=(eventStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(eventEndDate)))
      .groupBy(KpiConstants.memberidColName).agg(min(KpiConstants.serviceDateColName).alias(KpiConstants.serviceDateColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val eventSub1Df = oneVisitEventDf.as("df1").join(eventStep2Df.as("df2"), Seq(KpiConstants.memberidColName))
      .filter($"df1.${KpiConstants.serviceDateColName}"=!=($"df2.${KpiConstants.serviceDateColName}"))
      .withColumn(KpiConstants.secondServiceDateColName, when($"df1.${KpiConstants.serviceDateColName}".>($"df2.${KpiConstants.serviceDateColName}"), $"df1.${KpiConstants.serviceDateColName}")
        .otherwise($"df2.${KpiConstants.serviceDateColName}"))
      .select(KpiConstants.memberidColName, KpiConstants.secondServiceDateColName)

    //</editor-fold>

    //<editor-fold desc="First event Calculation with multiple eventStep1 VISITS">

    /*Two or more visits with the condition*/
    val eventStep3Df = eventStep1Df.except(eventStep1Df.filter($"${KpiConstants.memberidColName}"
      .isin(oneVisitEventDf.select(KpiConstants.memberidColName)
        .distinct()
        .rdd
        .map(r=> r.getString(0))
        .collect():_*)))


    val eventWindow = Window.partitionBy(KpiConstants.memberidColName).orderBy($"${KpiConstants.serviceDateColName}")

    /*Rank Column Added to the eventStep3Df*/
    val eventStep4Df = eventStep3Df.withColumn(KpiConstants.rankColName, rank().over(eventWindow))

    val eventSub2Df = eventStep4Df.filter($"${KpiConstants.rankColName}".===(2))
      .withColumnRenamed(KpiConstants.serviceDateColName, KpiConstants.secondServiceDateColName)
      .select(KpiConstants.memberidColName, KpiConstants.secondServiceDateColName)

    //</editor-fold>

    val eventDf = eventSub1Df.union(eventSub2Df)

    eventDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir+ "/eventDf")



    //</editor-fold>


    val totalPopulationVisitsDf = validVisitsDf.as("df1").join(eventDf.as("df2"), KpiConstants.memberidColName)


    totalPopulationVisitsDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir + "/totalPopulationVisitsDf/")

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

    val visitForMandExclDf = spark.read.parquet(intermediateDir + "/totalPopulationVisitsDf/").cache()
    visitForMandExclDf.count()
    val visitForMandExclInDf = visitForMandExclDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitForMandExclInDf.count()
    validVisitsDf.unpersist()

    //<editor-fold desc="Mandatory Exclusion1(Only for Medicare)">

    val ageCheckDate = year + "12-31"
    val mandExcl1InDf = visitForMandExclInDf.filter(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))
                                                     &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months792).<=(ageCheckDate)))
                                               .select(KpiConstants.memberidColName, KpiConstants.lobProductColName)

    val mandatoryExcl1Members = mandExcl1InDf.as("df1").join(medmonmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                         .select(s"df1.${KpiConstants.memberidColName}", KpiConstants.ltiFlagColName)
                                                         .groupBy(KpiConstants.memberidColName).agg(count(when($"${KpiConstants.ltiFlagColName}".===(KpiConstants.yesVal),1)).alias(KpiConstants.countColName))
                                                         .filter($"${KpiConstants.countColName}".>=(1))
                                                         .select(KpiConstants.memberidColName).rdd.map(r => r.getString(0)).collect()


    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion2">

    val mandatoryExcl2Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.fralityVal))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate)) && ($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months972).<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .distinct()

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion3">

    val prevYearStDate = year.toInt-1 +"-01-01"
    val mandatoryExcl3Sub1Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.fralityVal))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate)) && ($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months792).<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months972).>(yearEndDate))))
      .select(KpiConstants.memberidColName)
      .distinct()

    val outPatAndAdvIllDf = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.outPatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val obsAndAdvIllDf = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.observationVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val edAndAdvIllDf = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.edVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val nAcuteInPatAndAdvIllDf = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.nonAcuteInPatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val mandatoryExcl3Sub21Df = outPatAndAdvIllDf.union(obsAndAdvIllDf).union(edAndAdvIllDf).union(nAcuteInPatAndAdvIllDf)
      .groupBy(KpiConstants.memberidColName)
      .agg(countDistinct(KpiConstants.serviceDateColName).alias("count"))
      .filter($"count".>=(2))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl3Sub22Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.acuteInpatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)

    val mandatoryExcl3Sub23Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.dementiaMedicationVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl3Sub2Df = mandatoryExcl3Sub21Df.union(mandatoryExcl3Sub22Df).union(mandatoryExcl3Sub23Df)

    val mandatoryExcl3Df = mandatoryExcl3Sub1Df.intersect(mandatoryExcl3Sub2Df)


    //</editor-fold>

    /*Mandatory Exclusion condition where member has to remove*/
    val mandatoryExcl_1Df =mandatoryExcl2Df.union(mandatoryExcl3Df).dropDuplicates().cache()

    //</editor-fold>

    /*Member level Removal of Mandatory Exclusion*/
    val eligibleVisitsStep1Df = visitForMandExclDf.except(visitForMandExclDf.filter($"${KpiConstants.memberidColName}".isin(mandatoryExcl_1Df.rdd.map(r=> r.getString(0)).collect():_*)))


    /*Lob level Removal of Mandatory Exclusion*/
    val eligibleVisitsDf = eligibleVisitsStep1Df.except(eligibleVisitsStep1Df.filter(($"${KpiConstants.memberidColName}".isin(mandatoryExcl1Members:_*))
                                                                                   &&(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))
                                                                                   ||($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName)))))


    eligibleVisitsDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir + "/eligibleVisitsDf/")



    val epopDf = eligibleVisitsDf.select(KpiConstants.memberidColName).distinct().cache()
    epopDf.count()
   /* eligibleVisitsDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir + "/eligiblePop/")*/

    //</editor-fold>

    //<editor-fold desc="Denominator Calculation">

    val denominatorDf = epopDf
    //</editor-fold>

    //<editor-fold desc="Numerator Temporary Calculation">

    val numeratorInDf = spark.read.parquet(intermediateDir + "/eligibleVisitsDf/").repartition(2).cache()
    numeratorInDf.count()

    val toutStrDf = numeratorInDf.select($"${KpiConstants.memberidColName}".alias(KpiConstants.ncqaOutmemberIdCol),$"${KpiConstants.payerColName}".alias(KpiConstants.ncqaOutPayerCol)).dropDuplicates()
                                 .withColumn(KpiConstants.ncqaOutMeasureCol, lit(KpiConstants.cbpMeasureId)).cache()
    toutStrDf.count()

    //<editor-fold desc="Numerator Non Supplemental Data">

    val numeratorInNonSuppDf = numeratorInDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()

    val numVisitAndSystNonSupDf = numeratorInNonSuppDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nonAcuteInPatientVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.remotebpmVal)))
      &&((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicLt140Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicGtOrEq140Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.secondServiceDateColName}")))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val numVisitAndDiastNonSupDf = numeratorInNonSuppDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nonAcuteInPatientVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.remotebpmVal)))
      &&((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicLt80Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicBtwn8090Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicGt90Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.secondServiceDateColName}")))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val  joinedVisSysDiasNonSupDf = numVisitAndSystNonSupDf.as("df1").join(numVisitAndDiastNonSupDf.as("df2"), Seq(KpiConstants.memberidColName, KpiConstants.serviceDateColName))
      .groupBy(KpiConstants.memberidColName).agg(max(KpiConstants.serviceDateColName).alias(KpiConstants.serviceDateColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val numSysNonSupDf = numeratorInNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicLt140Val))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val numDiasNonSupDf = numeratorInNonSuppDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicBtwn8090Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicLt80Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val numTmpNonSupDf = joinedVisSysDiasNonSupDf.intersect(numSysNonSupDf).intersect(numDiasNonSupDf).select(KpiConstants.memberidColName)

    //</editor-fold>


    //<editor-fold desc="Numerator Other Data">

    val numeratorOtherDf = numeratorInDf.except(numeratorInDf.filter($"${KpiConstants.memberidColName}".isin(numTmpNonSupDf.rdd.map(r=>r.getString(0)).collect():_*)))



    val numVisitAndSystOtherDf = numeratorOtherDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nonAcuteInPatientVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.remotebpmVal)))
      &&((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicLt140Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicGtOrEq140Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.secondServiceDateColName}")))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val numVisitAndDiastOtherDf = numeratorOtherDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outpatwoUbrevVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nonAcuteInPatientVal))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.remotebpmVal)))
      &&((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicLt80Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicBtwn8090Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicGt90Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.secondServiceDateColName}")))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val  joinedVisSysDiasOtherDf = numVisitAndSystOtherDf.as("df1").join(numVisitAndDiastOtherDf.as("df2"), Seq(KpiConstants.memberidColName, KpiConstants.serviceDateColName))
      .groupBy(KpiConstants.memberidColName).agg(max(KpiConstants.serviceDateColName).alias(KpiConstants.serviceDateColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)


    val numSysOtherDf = numeratorOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.systolicLt140Val))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val numDiasOtherDf = numeratorOtherDf.filter(((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicBtwn8090Val))
      ||(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.diastolicLt80Val)))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val numTmpOtherDf = joinedVisSysDiasOtherDf.intersect(numSysOtherDf).intersect(numDiasOtherDf).select(KpiConstants.memberidColName)
    //</editor-fold>

    val numeratorTmpDf = numTmpNonSupDf.union(numTmpOtherDf)


    /*numeratorTmpDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir + "/numeratorTmpDf/")*/

    //</editor-fold>

    //<editor-fold desc="Optional exclusion Calculation">

    val optionalMemList = denominatorDf.except(numeratorTmpDf).rdd.map(r => r.getString(0)).collect()

    val optinalExclInDf = numeratorInDf.filter($"${KpiConstants.memberidColName}".isin(optionalMemList:_*)).cache()
    optinalExclInDf.count()

    val optionalExclNonSuppDf = optinalExclInDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    optionalExclNonSuppDf.count()


    //<editor-fold desc="Optional Exclusion1">

    val optExcl1NonSuppDf = optionalExclNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.esrdVal))
                                                       ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.esrdObsoleteVal))
                                                       ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.kidneyTransplantVal))
                                                       &&(($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                       &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
                                                 .select(KpiConstants.memberidColName)


    val optExcl1OtherDf = optinalExclInDf.except(optinalExclInDf.filter($"${KpiConstants.memberidColName}".isin(optExcl1NonSuppDf.rdd.map(r=> r.getString(0)).collect():_*)))
                                         .filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.esrdVal))
                                               ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.esrdObsoleteVal))
                                               ||(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.kidneyTransplantVal))
                                               &&(($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                               &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
                                         .select(KpiConstants.memberidColName)

    val optExcl1Df = optExcl1NonSuppDf.union(optExcl1OtherDf)
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion2">

    val optExcl2NonSuppDf = optionalExclNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.pregnancyVal))
                                                       &&($"${KpiConstants.genderColName}".===(KpiConstants.femaleVal))
                                                       &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
                                                       &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
                                                  .select(KpiConstants.memberidColName)


    val optExcl2OtherDf = optinalExclInDf.except(optinalExclInDf.filter($"${KpiConstants.memberidColName}".isin(optExcl2NonSuppDf.rdd.map(r=> r.getString(0)).collect():_*)))
                                         .filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.pregnancyVal))
                                               &&($"${KpiConstants.genderColName}".===(KpiConstants.femaleVal))
                                               &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
                                                &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate))))
                                         .select(KpiConstants.memberidColName)

    val optExcl2Df = optExcl2NonSuppDf.union(optExcl2OtherDf)
    //</editor-fold>

    //<editor-fold desc="Optional Exclusion3">

    val optExcl3NonSuppDf = optionalExclNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.inpatientStayVal))
                                                       &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.nonacuteInPatStayVal))
                                                       &&(($"${KpiConstants.admitDateColName}".>=(yearStartDate))
                                                       &&($"${KpiConstants.admitDateColName}".<=(yearEndDate))))
                                                 .select(KpiConstants.memberidColName)


    val optExcl3OtherDf = optinalExclInDf.except(optinalExclInDf.filter($"${KpiConstants.memberidColName}".isin(optExcl3NonSuppDf.rdd.map(r=> r.getString(0)).collect():_*)))
                                         .filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.inpatientStayVal))
                                               &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.nonacuteInPatStayVal))
                                               &&(($"${KpiConstants.admitDateColName}".>=(yearStartDate))
                                               &&($"${KpiConstants.admitDateColName}".<=(yearEndDate))))
                                         .select(KpiConstants.memberidColName)

    val optExcl3Df = optExcl3NonSuppDf.union(optExcl3OtherDf)
    //</editor-fold>


    val optExclDf = optExcl1Df.union(optExcl2Df).union(optExcl3Df).dropDuplicates().cache()

   /* optExclDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir + "/optExclDf/")*/

    //</editor-fold>

    /*Numerator Calculation*/
    val numeratorDf = numeratorTmpDf.except(optExclDf)

    //<editor-fold desc="Ncqa Output Creation">

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutStrDf, KpiConstants.eligibleDfName -> denominatorDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> optExclDf,
      KpiConstants.numeratorDfName -> numeratorDf)


    val outDf = UtilFunctions.ncqaOutputDfCreation(spark,outMap,measureId)
    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir+ "/outDf")
    //</editor-fold>

    //<editor-fold desc="Deleting the intermediate Files">

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(intermediateDir), true)
    //</editor-fold>

    spark.sparkContext.stop()


  }

}
