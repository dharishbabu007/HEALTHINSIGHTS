package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
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


    /*calling function for setting the dbname for dbName variable*/
    KpiConstants.setDbName(dbName)
    val conf = new SparkConf().setAppName("NcqaProgram")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")


    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    val ystDate = year+"-01-01"
    val yendDate = year+"-12-31"
    val aLiat = List("col1")
    val msrVal = s"'$measureId'"
    val ggMsrId = s"'${KpiConstants.ggMeasureId}'"
    val lobList = List(KpiConstants.commercialLobName,KpiConstants.medicareLobName, KpiConstants.medicaidLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
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

    //<editor-fold desc="Age Filter Calculation">

    val ageClDate = year+ "-12-31"
    val ageAndGenderFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months624).<=(ageClDate))
                                                 &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months900).>(ageClDate))
                                                 &&($"${KpiConstants.genderColName}".===(KpiConstants.femaleVal)))
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap Calculation">

    val inputForContEnrolldf = ageAndGenderFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val mapForCeminus2y = mutable.Map("start_date" -> "2016-10-01", "end_date" -> "2016-12-31","gapcount" -> "0",
                                      "checkval" -> "false")

    val ceOutminus2yDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,inputForContEnrolldf,mapForCeminus2y)


    val mapForCeminus1y = mutable.Map("start_date" -> "2017-01-01", "end_date" -> "2017-12-31","gapcount" -> "1",
                                      "checkval" -> "false")

    val ceOutminus1yDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,ceOutminus2yDf,mapForCeminus1y)


    val mapForCeCurrYear = mutable.Map("start_date" -> "2018-01-01", "end_date" -> "2018-12-31","gapcount" -> "1",
                                        "checkval" -> "true","anchor_date" -> "2018-12-31")

    val contEnrollDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,ceOutminus1yDf,mapForCeCurrYear)
                                    .select(KpiConstants.memberidColName, KpiConstants.memStartDateColName, KpiConstants.memEndDateColName,
                                            KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.primaryPlanFlagColName,
                                            KpiConstants.payerColName)

    //</editor-fold>

    //<editor-fold desc="Dual Enrollment calculation">

    val baseOutDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList)
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

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate)
      && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .dropDuplicates()
      .rdd
      .map(r=> r.getString(0))
      .collect()


    val hospiceRemMemEnrollDf = baseOutDf.except(baseOutDf.filter($"${KpiConstants.memberidColName}".isin(hospiceInCurrYearMemDf:_*)))
    hospiceRemMemEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/hospiceRemMemEnrollDf/")

    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> hospiceRemMemEnrollDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val validVisitsOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).repartition(2).cache()
    validVisitsOutDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/hbase/ncqa/cbp_test_out/validVisitsOutDf/")

    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

    //<editor-fold desc="Mandatory Exclusion1(Only for Medicare)">

    val ageCheckDate = year + "12-31"
    val mandExcl1InDf = validVisitsOutDf.filter(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))
                                              &&(UtilFunctions.add_ncqa_months(spark,$"df1.${KpiConstants.dobColName}",KpiConstants.months792).<=(ageCheckDate)))
                                        .select(KpiConstants.memberidColName, KpiConstants.lobProductColName)

    val mandatoryExcl1Df = mandExcl1InDf.as("df1").join(medmonmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select(s"df1.${KpiConstants.memberidColName}", KpiConstants.lobProductColName, KpiConstants.ltiFlagColName)
      .groupBy(KpiConstants.memberidColName).agg(first(KpiConstants.lobProductColName).alias(KpiConstants.lobProductColName),
      count(when($"${KpiConstants.ltiFlagColName}".===("Y"),1)).alias("count"))
      .filter(($"${KpiConstants.lobProductColName}".===(KpiConstants.lobProductNameConVal))
        ||($"count".>=(1)))
      .select(KpiConstants.memberidColName)


    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion3">

    val prevYearStDate = year.toInt-1 +"-01-01"
    val mandatoryExcl3Sub1Df = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.fralityVal))
      &&(($"${KpiConstants.serviceDateColName}".>=(yearStartDate)) && ($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      &&((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months792).<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months972).>(yearEndDate))))
      .select(KpiConstants.memberidColName)
      .distinct()

    val outPatAndAdvIllDf = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.outPatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val obsAndAdvIllDf = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.observationVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val edAndAdvIllDf = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.edVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val nAcuteInPatAndAdvIllDf = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.nonAcuteInPatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)



    val mandatoryExcl3Sub21Df = outPatAndAdvIllDf.union(obsAndAdvIllDf).union(edAndAdvIllDf).union(nAcuteInPatAndAdvIllDf)
      .groupBy(KpiConstants.memberidColName)
      .agg(countDistinct(KpiConstants.serviceDateColName).alias("count"))
      .filter($"count".>=(2))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl3Sub22Df = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.acuteInpatientVal))
      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.advancedIllVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)

    val mandatoryExcl3Sub23Df = validVisitsOutDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.dementiaMedicationVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)


    val mandatoryExcl3Sub2Df = mandatoryExcl3Sub21Df.union(mandatoryExcl3Sub22Df).union(mandatoryExcl3Sub23Df)

    val mandatoryExcl3Df = mandatoryExcl3Sub1Df.intersect(mandatoryExcl3Sub2Df)

    mandatoryExcl3Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/cbp_test_out/mandatoryExcl3Df/")

    //</editor-fold>

    val mandatoryExclDf = mandatoryExcl1Df.union(mandatoryExcl3Df)

    //</editor-fold>

    val eligPopDf = hospiceRemMemEnrollDf.except(hospiceRemMemEnrollDf.filter($"${KpiConstants.memberidColName}".isin(mandatoryExclDf.rdd.map(r=>r.getString(0)).collect():_*)))

    spark.sparkContext.stop()

  }
}
