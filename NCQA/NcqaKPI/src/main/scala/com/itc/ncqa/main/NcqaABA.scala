package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConversions._
import scala.collection.mutable

object NcqaABA {


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
      .set("spark.sql.shuffle.partitions","5")


    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val prevYearStDate = year.toInt-1 +"-01-01"

    val yearNovDate = year+"-11-01"
    val oct1Date = year.toInt-2 +"-10-01"
    val jobId = spark.sparkContext.applicationId
    val baseDir = baseMsrPath + "/" + jobId
    val outDir = baseDir + "/Out"
    val intermediateDir = baseDir + "/Intermediate"

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    val aLiat = List("col1")
    val msrVal = s"'$measureId'"
    val ggMsrId = s"'${KpiConstants.ggMeasureId}'"
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName, KpiConstants.medicareLobName, KpiConstants.marketplaceLobName)
    val memqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.membershipTblName} WHERE measure = $msrVal AND (member_plan_start_date IS  NOT NULL) AND(member_plan_end_date IS NOT NULL)"""
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,memqueryString,aLiat)
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(2)





    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val visitqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.visitTblName} WHERE measure = $msrVal and  (service_date IS  NOT NULL) AND((admit_date IS NULL and discharge_date IS NULL) OR (ADMIT_DATE IS NOT NULL AND DISCHARGE_DATE IS NOT NULL))"""
    val visitsDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,visitqueryString,aLiat)
      .filter((($"${KpiConstants.dataSourceColName}".===("Claim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
        ||(($"${KpiConstants.dataSourceColName}".===("RxClaim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
        ||($"${KpiConstants.dataSourceColName}".===("Rx")))
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

    //<editor-fold desc="Age Filter">

    val ageFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",216).<=(prevYearStDate))
                                        &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",900).>(yearEndDate))).cache()

   /* ageFilterDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/hbase/ncqa/aba/ageFilterDf/")*/

    /*println("-------------------Age Filter----------------")
    ageFilterDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap Calculation">

   //val ageDf = spark.read.parquet("/home/hbase/ncqa/aba/ageFilterDf/").cache()

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname, KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
                                            KpiConstants.genderColName, KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,
                                            KpiConstants.primaryPlanFlagColName)

    val mapForCeminus2y = mutable.Map("start_date" -> (year.toInt-1+ "-01-01"), "end_date" -> (year.toInt-1+ "-12-31"),"gapcount" -> "1",
                                      "reqCovDays" -> "0", "checkval" -> "false")

    val ceOutminus2yDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,inputForContEnrolldf,mapForCeminus2y)

    val mapForCeminus1y = mutable.Map("start_date" -> (year+"-01-01"), "end_date" -> (year+"-12-31"),"gapcount" -> "1",
                                      "reqCovDays" -> "0", "checkval" -> "true","anchor_date" -> (year+"-12-31"))

    val contEnrollDf = UtilFunctions.contEnrollAndAllowableGapFilterFunction(spark,ceOutminus2yDf,mapForCeminus1y)
                                    .select(KpiConstants.memberidColName, KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.primaryPlanFlagColName)
                                    .repartition(3).cache()

   /* contEnrollDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/hbase/ncqa/aba/contEnrollDf/")*/

    /*println("-------------------Continous Enrollment----------------")
    contEnrollDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>

    //<editor-fold desc="Dual eligibility,Dual enrollment, AWC enrollment filter">

   // val ConEnrDf = spark.read.parquet("/home/hbase/ncqa/aba/contEnrollDf/").cache()

    val baseOutInDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList,measureId)

    /*Including the Reporting payers and Lob*/
    val abaContEnrollDf = baseOutInDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*)).dropDuplicates().cache()
    abaContEnrollDf.count()

    //Modified Code
    val medicareContEnrollDf = abaContEnrollDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))

    val medicareHospiceDf = medicareContEnrollDf.as("df1").join(medmonmemDf.as("df2"), Seq(KpiConstants.memberidColName))
      .groupBy(KpiConstants.memberidColName).agg(countDistinct(when($"hospice".===("Y"),1)).alias("count"))
      .filter($"count".>(0))
      .select(KpiConstants.memberidColName).rdd.map(r=> r.getString(0)).collect()

    val abacontEnrollResDf = abaContEnrollDf.except(abaContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(medicareHospiceDf:_*)))

    val snPayerList = List(KpiConstants.sn1PayerVal, KpiConstants.sn2PayerVal, KpiConstants.sn3PayerVal, KpiConstants.mmpPayerVal)
    val baseOutDf = abacontEnrollResDf.except(abacontEnrollResDf.filter($"${KpiConstants.payerColName}".isin(snPayerList:_*)))

    baseOutDf.coalesce(3)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+"/baseOutDf/")

   /* println("-------------------After Dual Enrollment----------------")
    abacontEnrollResDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()
*/

    //Ends here

    //</editor-fold>

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Initial Join with Ref_Hedis">

   /* val VisDf = spark.read.parquet("/home/hbase/ncqa/aba/visitsDf/").cache()

    val RefHedDf = spark.read.parquet("/home/hbase/ncqa/aba/refHedisDf/").cache()*/

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.outPatientVal,KpiConstants.bmiVal,
      KpiConstants.bmiPercentileVal,KpiConstants.pregnancyVal)

    val medList = KpiConstants.emptyList
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medList)

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    //val VisRefHedDf = spark.read.parquet("/home/hbase/ncqa/aba/visitRefHedisDf/").cache()

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,KpiConstants.genderColName,
        KpiConstants.dobColName,KpiConstants.supplflagColName, KpiConstants.ispcpColName,KpiConstants.isobgynColName, KpiConstants.valuesetColName)


    val indLabVisRemDf = visitgroupedDf.filter(!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal))
                                       .repartition(2).cache()

    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

   /* val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val yearStartDate1 = year.toInt - 1 +"-01-01"*/

    val ConEnrResDf = spark.read.parquet(intermediateDir+"/baseOutDf/").cache()

    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate)
      && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .dropDuplicates()
      .rdd
      .map(r=> r.getString(0))
      .collect()

    val hospiceRemMemEnrollDf = ConEnrResDf.except(ConEnrResDf.filter($"${KpiConstants.memberidColName}".isin(hospiceInCurrYearMemDf:_*)))

    /*hospiceRemMemEnrollDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/hbase/ncqa/aba/hospiceRemMemEnrollDf/")

    println("------------------After Hospice Removal----------------")
    hospiceRemMemEnrollDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>

    //<editor-fold desc="Initial join function">

   // val HosRemMemEnrDf = spark.read.parquet("/home/hbase/ncqa/aba/hospiceRemMemEnrollDf/").cache()

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> hospiceRemMemEnrollDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val validVisitsOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin)

    validVisitsOutDf.coalesce(3)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+"/validVisitsOutDf/")

    //</editor-fold>

    //<editor-fold desc="Eligible Event calculation">

    val validVisitsDf = spark.read.parquet(intermediateDir+"/validVisitsOutDf/").repartition(2).cache()
    validVisitsDf.count()

    val visitForEventDf = validVisitsDf.filter($"${KpiConstants.supplflagColName}".===("N"))


   // println("-----------------------validVisitsDf-----------------------")
    //validVisitsDf.show()

    //<editor-fold desc="Event Calculation">

   /* val eventStartDate = year.toInt -1 +"-01-01"
    val eventEndDate = year+"-12-31"*/

    /*Find out the memebers who has Outpatient */
    val eventDf = visitForEventDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outPatientVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate))
      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)

   /* eventDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/hbase/ncqa/aba/eventDf/")


    println("---------------eventDf-------------------")*/

    //</editor-fold>

   /* val EvenDf = spark.read.parquet("/home/hbase/ncqa/aba/eventDf/").repartition(2).cache()*/

    val totalPopulationVisitsDf = validVisitsDf.as("df1").join(eventDf.as("df2"), KpiConstants.memberidColName)
      .select(s"df1.*")

    val totalPopDf = totalPopulationVisitsDf.select($"${KpiConstants.memberidColName}",$"${KpiConstants.memberidColName}".as(KpiConstants.ncqaOutmemberIdCol),
                                                          $"${KpiConstants.payerColName}".as(KpiConstants.ncqaOutPayerCol))
                                            .withColumn(KpiConstants.ncqaOutMeasureCol,lit(measureId)).distinct()

    val eligiblePopDf = totalPopulationVisitsDf.select(KpiConstants.memberidColName).distinct().cache()

   /* totalPopulationVisitsDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/aba/totalPopulationVisitsDf/")

    println("---------------totalPopulationVisitsDf-------------------")
    totalPopulationVisitsDf.show()*/

    //</editor-fold>

    //<editor-fold desc="Denominator calculation">

    totalPopulationVisitsDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+"/totalPopulationVisitsDf/")

    val denominatorDf = eligiblePopDf.distinct()

   /* println("---------------denominatorDf-------------------")
    denominatorDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>

    //</editor-fold>

    val visitsForNumDf = spark.read.parquet(intermediateDir+"/totalPopulationVisitsDf/").cache()
    visitsForNumDf.count()

    //<editor-fold desc="ABA tmp Numerator Calculation">

    val visitNonSuppDf = visitsForNumDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitNonSuppDf.count()
   /* println("---------------------------visitNonSuppDf-------------------------")
    visitNonSuppDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //<editor-fold desc="ABA BMI tmp Numerator Calculation">

    val abaBmiNonSuppDf = visitNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",240).<=($"${KpiConstants.serviceDateColName}")))
      .select(s"${KpiConstants.memberidColName}").cache()

    /*println("---------------------------abaBmiNonSuppDf-------------------------")
    abaBmiNonSuppDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val abaBmiOtherVisitsDf = visitsForNumDf.except(visitsForNumDf
      .filter($"${KpiConstants.memberidColName}".isin(abaBmiNonSuppDf.rdd.map(r=> r.getString(0)).collect():_*)))

   /* println("---------------------------abaBmiOtherVisitsDf-------------------------")
    abaBmiOtherVisitsDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val abaBmiMenOtherDf = abaBmiOtherVisitsDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",240).<=($"${KpiConstants.serviceDateColName}"))
    )
      .select(s"${KpiConstants.memberidColName}")


   /* println("---------------------------abaBmiMenOtherDf-------------------------")
    abaBmiMenOtherDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val abaBmiMenTmpDf = abaBmiNonSuppDf.union(abaBmiMenOtherDf).dropDuplicates().cache()

   /* println("---------------------------abaBmiMenTmpDf-------------------------")
    abaBmiMenTmpDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>

    //<editor-fold desc="ABA BMI Percentile tmp Numerator Calculation">

    val abaBmiPercNonSuppDf = visitNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiPercentileVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",240).>($"${KpiConstants.serviceDateColName}"))
    )
      .select(s"${KpiConstants.memberidColName}").cache()

   /* println("---------------------------abaBmiPercNonSuppDf-------------------------")
    abaBmiPercNonSuppDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val abaBmiPercOtherVisitsDf = visitsForNumDf.except(visitsForNumDf.filter($"${KpiConstants.memberidColName}".isin(abaBmiPercNonSuppDf.
      rdd.map(r=> r.getString(0)).collect():_*)))

    val abaBmiPercMenOtherDf = abaBmiPercOtherVisitsDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiPercentileVal))
      &&($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))
      &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",240).>($"${KpiConstants.serviceDateColName}"))
    )
      .select(s"${KpiConstants.memberidColName}")

   /* abaBmiPercMenOtherDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val abaBmiPercMenTmpDf = abaBmiPercNonSuppDf.union(abaBmiPercMenOtherDf).dropDuplicates().cache()

   /* println("---------------------------abaBmiPercMenTmpDf-------------------------")
    abaBmiPercMenTmpDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    //</editor-fold>


    val abiTmpOutput = abaBmiMenTmpDf.union(abaBmiPercMenTmpDf)


   /* println("---------------------------abiTmpOutput-------------------------")
    abiTmpOutput.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/


    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

   /* println("---------------------------totalNum-------------------------")
    NumIntersect.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val visitForOptExclDf = visitsForNumDf.except(visitsForNumDf.filter($"${KpiConstants.memberidColName}".isin(abiTmpOutput.rdd.map(r=> r.getString(0)).collect():_*))).cache()
    visitForOptExclDf.count()


   /* println("---------------------------visitForOptExclDf-------------------------")
    visitForOptExclDf.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

   /* visitForOptExclDf.select($"${KpiConstants.memberidColName}")
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/aba/visitForOptExclDf")*/

    //<editor-fold desc="Optional Exclusion Calculation For Non Supplementry And Other Data">


    val abaOptExclNonSupp= visitForOptExclDf.filter(($"${KpiConstants.genderColName}".===("F")) && ($"${KpiConstants.supplflagColName}".===("N")) &&
      (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.pregnancyVal)) && ($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) &&
      $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)

    val abaOptExclSuppInput = visitForOptExclDf.except(visitForOptExclDf.filter($"${KpiConstants.memberidColName}".isin(
      abaOptExclNonSupp.rdd.map(r=>r.getString(0)).collect():_*)))

    val abaOptExclSupp= abaOptExclSuppInput.filter(($"${KpiConstants.genderColName}".===("F")) &&
      (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.pregnancyVal))
      && ($"${KpiConstants.serviceDateColName}".>=(prevYearStDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)

    val abaOptExclDf=abaOptExclNonSupp.union(abaOptExclSupp).distinct()


    //</editor-fold>

   /* abaOptExclDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/aba/abaOptExclDf/")

    println("After Optional Exclusion")
    abaOptExclDf.filter($"${KpiConstants.memberidColName}" === ("100018")).show()*/


    //</editor-fold>

    val num1 = abaBmiMenTmpDf.except(abaOptExclDf)

    /*println("Num1")
    Num1.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/

    val num2 = abaBmiPercMenTmpDf.except(abaOptExclDf)

   /* println("Num2")

    num1.filter($"${KpiConstants.memberidColName}".===("100018")).show()

    num2.filter($"${KpiConstants.memberidColName}".===("100018")).show()*/

    val finalNum = num1.union(num2).dropDuplicates().cache()


    //<editor-fold desc="ABA NCQA Output creation">

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> totalPopDf, KpiConstants.eligibleDfName -> denominatorDf,
                             KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> abaOptExclDf,
                             KpiConstants.numeratorDfName -> finalNum)


    val outDf = UtilFunctions.ncqaOutputDfCreation(spark,outMap,measureId)

    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir+"/outDf")
    //</editor-fold>

    //<editor-fold desc="Deleting the intermediate Files">

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(intermediateDir), true)
    //</editor-fold>


    spark.sparkContext.stop()

  }
}
