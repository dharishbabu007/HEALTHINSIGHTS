package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object NcqaBCS {

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

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val prevYearStDate = year.toInt-1 +"-01-01"
    val jobId = spark.sparkContext.applicationId
    val baseDir = baseMsrPath + "/" + jobId
    val outDir = baseDir + "/Out"
    val intermediateDir = baseDir + "/Intermediate"

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">


    val aLiat = List("col1")
    val msrVal = s"'$measureId'"
    val ggMsrId = s"'${KpiConstants.ggMeasureId}'"
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName,KpiConstants.medicareLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)

    val memqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.membershipTblName} WHERE measure = $msrVal AND (member_plan_start_date IS  NOT NULL) AND(member_plan_end_date IS NOT NULL)"""
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,memqueryString,aLiat)
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(2)

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
      .filter(($"run_date".>=(yearStartDate)) && ($"run_date".<=(yearEndDate)))
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(2)

    val refHedisqueryString =s"""SELECT * FROM  ${KpiConstants.dbName}.${KpiConstants.refHedisTblName} WHERE measureid = $msrVal OR measureid= $ggMsrId"""
    val refHedisDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,refHedisqueryString,aLiat)
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


    val refMedqueryString = s"""SELECT * FROM  ${KpiConstants.dbName}.${KpiConstants.refmedValueSetTblName} WHERE measure_id = $msrVal"""
    val ref_medvaluesetDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,refMedqueryString,aLiat)
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


    // println("counts:"+membershipDf.count()+","+ visitsDf.count()+","+medmonmemDf.count() +","+refHedisDf.count()+","+ref_medvaluesetDf.count())

    //</editor-fold

    //<editor-fold desc=" Eligible Population Calculation">

    //<editor-fold desc="Age Filter And SN2 Removal Calculation">

    val ageClDate = year+ "-12-31"
    val ageAndGenderFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months624).<=(ageClDate))
                                                 &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months900).>(ageClDate))
                                                 &&($"${KpiConstants.genderColName}".===(KpiConstants.femaleVal)))

    val sn2Members = ageAndGenderFilterDf.filter(($"${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))
      &&(((($"${KpiConstants.memStartDateColName}".>=(yearStartDate)) && ($"${KpiConstants.memStartDateColName}".<=(yearEndDate)))
      ||(($"${KpiConstants.memEndDateColName}".>=(yearStartDate)) && ($"${KpiConstants.memEndDateColName}".<=(yearEndDate))))
      || ((($"${KpiConstants.memStartDateColName}".<(yearStartDate)) && ($"${KpiConstants.memEndDateColName}".>(yearEndDate)))))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months792).<=(yearEndDate)))
      .select(KpiConstants.memberidColName).rdd.map(r => r.getString(0)).collect()
    val sn2RemovedMemDf = ageAndGenderFilterDf.except(ageAndGenderFilterDf.filter($"${KpiConstants.memberidColName}".isin(sn2Members:_*)))

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap Calculation">

    val inputForContEnrolldf = ageAndGenderFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val mapForCeminus2y = mutable.Map("start_date" -> "2016-10-01", "end_date" -> "2016-12-31","gapcount" -> "0",
                                      "checkval" -> "false", "reqCovDays" -> "92")

    val ceOutminus2yDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,inputForContEnrolldf,mapForCeminus2y)


    val mapForCeminus1y = mutable.Map("start_date" -> "2017-01-01", "end_date" -> "2017-12-31","gapcount" -> "1",
                                      "checkval" -> "false", "reqCovDays" -> "0")

    val ceOutminus1yDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,ceOutminus2yDf,mapForCeminus1y)


    val mapForCeCurrYear = mutable.Map("start_date" -> "2018-01-01", "end_date" -> "2018-12-31","gapcount" -> "1",
                                        "checkval" -> "true","reqCovDays" -> "0","anchor_date" -> "2018-12-31")

    val contEnrollDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,ceOutminus1yDf,mapForCeCurrYear)
                                    .select(KpiConstants.memberidColName, KpiConstants.memStartDateColName, KpiConstants.memEndDateColName,
                                            KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.primaryPlanFlagColName,
                                            KpiConstants.payerColName).repartition(2).cache()

    contEnrollDf.count()

    //</editor-fold>

    //<editor-fold desc="Dual Enrollment calculation">

    val baseOutInDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList,measureId)
    /*baseOutInDf.coalesce(5)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/baseOutInDf/")
*/
    val medicareContEnrollDf = baseOutInDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName)).repartition(2).cache()
    medicareContEnrollDf.count()

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

    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal, KpiConstants.fralityVal,
                            KpiConstants.outPatientVal, KpiConstants.advancedIllVal, KpiConstants.observationVal,
                            KpiConstants.edVal, KpiConstants.nonAcuteInPatientVal, KpiConstants.acuteInpatientVal,
                            KpiConstants.mammographyVal, KpiConstants.bilateralMastectomyVal, KpiConstants.bilateralModifierVal,
                            KpiConstants.historyBilateralMastectomyVal, KpiConstants.leftModifierVal, KpiConstants.rightModifierVal,
                            KpiConstants.unilateralMastectomyLeftVal, KpiConstants.unilateralMastectomyRightVal)

    val medicationlList = List(KpiConstants.dementiaMedicationVal)
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medicationlList)

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
      .select(KpiConstants.memberidColName,KpiConstants.dobColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
        KpiConstants.supplflagColName, KpiConstants.valuesetColName)

    val indLabVisRemDf = visitgroupedDf.filter((!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)))
                                       .repartition(2).cache()
    indLabVisRemDf.count()
    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val baseMemDf = spark.read.parquet(intermediateDir+ "/baseOutDf/").repartition(2)
    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
                                                     &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate))
                                                     &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                               .select(KpiConstants.memberidColName)
                                               .dropDuplicates()
                                               .rdd
                                               .map(r=> r.getString(0))
                                               .collect()


    val hospiceRemMemEnrollDf = baseMemDf.except(baseMemDf.filter($"${KpiConstants.memberidColName}".isin(hospiceInCurrYearMemDf:_*)))
    hospiceRemMemEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir +"/hospiceRemMemEnrollDf/")

    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> hospiceRemMemEnrollDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val validVisitsOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).repartition(2).cache()
    validVisitsOutDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/validVisitsOutDf/")

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

    val visitForMandExclDf = spark.read.parquet(intermediateDir+ "/validVisitsOutDf/").cache()
    visitForMandExclDf.count()
    val visitForMandExclInDf = visitForMandExclDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitForMandExclInDf.count()


    //<editor-fold desc="Mandatory Exclusion1(Only for Medicare)">

    val mandExcl1InDf = visitForMandExclInDf.filter(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))
                                                  &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months792).<=(yearEndDate)))
                                            .select(KpiConstants.memberidColName, KpiConstants.lobProductColName)

    val mandatoryExcl1Members = mandExcl1InDf.as("df1").join(medmonmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                              .select(s"df1.${KpiConstants.memberidColName}", KpiConstants.ltiFlagColName)
                                                              .groupBy(KpiConstants.memberidColName).agg(count(when($"${KpiConstants.ltiFlagColName}".===(KpiConstants.yesVal),1)).alias(KpiConstants.countColName))
                                                              .filter($"${KpiConstants.countColName}".>=(1))
                                                              .select(KpiConstants.memberidColName).rdd.map(r => r.getString(0)).collect()


    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion2">


    val mandatoryExcl2Sub1Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.fralityVal))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate)) && ($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months792).<=(yearEndDate)))
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



    val mandatoryExcl2Sub21Df = outPatAndAdvIllDf.union(obsAndAdvIllDf).union(edAndAdvIllDf).union(nAcuteInPatAndAdvIllDf)
      .groupBy(KpiConstants.memberidColName)
      .agg(countDistinct(KpiConstants.serviceDateColName).alias("count"))
      .filter($"count".>=(2))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl2Sub22Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.acuteInpatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)

    val mandatoryExcl2Sub23Df = visitForMandExclInDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.dementiaMedicationVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl2Sub2Df = mandatoryExcl2Sub21Df.union(mandatoryExcl2Sub22Df).union(mandatoryExcl2Sub23Df)

    val mandatoryExcl2Df = mandatoryExcl2Sub1Df.intersect(mandatoryExcl2Sub2Df)

    //</editor-fold>

    //</editor-fold>

    /*Member level Removal of Mandatory Exclusion*/
    val eligibleVisitsStep1Df = visitForMandExclDf.except(visitForMandExclDf.filter($"${KpiConstants.memberidColName}".isin(mandatoryExcl2Df.rdd.map(r=> r.getString(0)).collect():_*)))

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
     eligibleVisitsDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
       .write
       .mode(SaveMode.Append)
       .option("header", "true")
       .csv(outDir + "/eligiblePop/")
    //</editor-fold>

    //<editor-fold desc="Deleting the intermediate Files">

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(intermediateDir), true)
    //</editor-fold>

    spark.sparkContext.stop()

  }
}
