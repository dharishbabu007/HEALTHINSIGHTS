package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{first, _}
import org.apache.spark.sql.types.{IntegerType, StringType}

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
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName,KpiConstants.medicareLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)

    val memqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.membershipTblName} WHERE measure = $msrVal AND (member_plan_start_date IS  NOT NULL) AND(member_plan_end_date IS NOT NULL)"""
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,memqueryString,aLiat)
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(1)

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
      .repartition(3)

    val medmonmemqueryString = s"""SELECT * FROM ${KpiConstants.dbName}.${KpiConstants.medmonmemTblName} WHERE measure = $msrVal"""
    val medmonmemDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,medmonmemqueryString,aLiat)
      .filter(($"run_date".>=(yearStartDate)) && ($"run_date".<=(yearEndDate)))
      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
      .repartition(1)

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

    val inputForContEnrolldf = sn2RemovedMemDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
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
                                    .select(KpiConstants.memberidColName, KpiConstants.dateofbirthColName, KpiConstants.memStartDateColName, KpiConstants.memEndDateColName,
                                            KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.primaryPlanFlagColName,
                                            KpiConstants.payerColName)

    contEnrollDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/contEnrollDf/")

    //</editor-fold>

    //<editor-fold desc="Dual Enrollment calculation">

    val contDf = spark.read.parquet(intermediateDir+ "/contEnrollDf/")
    val baseOutInDf = UtilFunctions.baseOutDataframeCreation(spark, contDf, lobList,measureId)

    val medicareContEnrollDf = baseOutInDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName)).repartition(2).cache()
    medicareContEnrollDf.count()

    /*Find out the Medicare Hospice Mmebers by lookung in the hospice flag in medicare_monthly_membership table*/
    val medicareHospiceDf = medicareContEnrollDf.as("df1").join(medmonmemDf.as("df2"), Seq(KpiConstants.memberidColName))
      .groupBy(KpiConstants.memberidColName).agg(countDistinct(when($"${KpiConstants.hospiceFlagColName}".===(KpiConstants.yesVal),1)).alias(KpiConstants.countColName))
      .filter($"${KpiConstants.countColName}".>(0))
      .select(KpiConstants.memberidColName).rdd.map(r=> r.getString(0)).collect()

    /*Remove the members who has lti flag inmonthly_medicare_membership table*/
    val baseOutStep1Df = baseOutInDf.except(baseOutInDf.filter($"${KpiConstants.memberidColName}".isin(medicareHospiceDf:_*)))

    /*Find out the Medicare  Members who has atleast 1 lti_flag in medicare_monthly_membership table*/
    val medicareLtiDf = medicareContEnrollDf.as("df1").join(medmonmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                             .select(s"df1.${KpiConstants.memberidColName}",KpiConstants.dateofbirthColName, KpiConstants.ltiFlagColName)
                                                             .filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months792).<=(yearEndDate)))
                                                             .groupBy(KpiConstants.memberidColName).agg(count(when($"${KpiConstants.ltiFlagColName}".===(KpiConstants.yesVal),1)).alias(KpiConstants.countColName))
                                                             .filter($"${KpiConstants.countColName}".>=(1))
                                                             .select(KpiConstants.memberidColName).rdd.map(r => r.getString(0)).collect()

    /*Remove the member's medicare and Medicare-Medicaid Plans data whao has atleast 1 LTI flag*/
    val baseOutMedHosRemDf = baseOutStep1Df.except(baseOutStep1Df.filter(($"${KpiConstants.memberidColName}".isin(medicareLtiDf:_*))
                                                             &&(($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))
                                                              ||($"${KpiConstants.lobColName}".===(KpiConstants.mmdLobName)))))

    /*Removing the SN payer members from the EPOP*/
    val snPayerList = List(KpiConstants.sn1PayerVal, KpiConstants.sn2PayerVal, KpiConstants.sn3PayerVal, KpiConstants.mmpPayerVal)
    val baseOutDf = baseOutMedHosRemDf.except(baseOutMedHosRemDf.filter($"${KpiConstants.payerColName}".isin(snPayerList:_*)))
    baseOutDf.coalesce(3)
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
                            KpiConstants.unilateralMastectomyVal,KpiConstants.historyBilateralMastectomyVal, KpiConstants.leftModifierVal, KpiConstants.rightModifierVal,
                            KpiConstants.unilateralMastectomyLeftVal, KpiConstants.unilateralMastectomyRightVal, KpiConstants.absOfLeftBreastVal, KpiConstants.absOfRightBreastVal )

    val medicationlList = List(KpiConstants.dementiaMedicationVal)
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medicationlList)

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

   // val listVal = List("159100", "100485", "110113", "111083", "117791")
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
                                         .select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.memStartDateColName).dropDuplicates()
   /* hospiceRemMemEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir +"/hospiceRemMemEnrollDf/")*/

    //</editor-fold>

    //<editor-fold desc="Stratification code">

    /*Stratification code*/
    val stratificationInputDf = hospiceRemMemEnrollDf.filter($"${KpiConstants.lobColName}".===(KpiConstants.medicareLobName))

   /* println("------------------------stratificationInputDf----------------------------")
    stratificationInputDf.filter($"${KpiConstants.memberidColName}".isin(List("101677","100879"):_*)).show()*/




    /*Step1*/
    val joinWithMedMonDf = stratificationInputDf.as("df1").join(medmonmemDf.as("df2"), Seq(KpiConstants.memberidColName))

   /* println("------------------------joinWithMedMonDf----------------------------")
    joinWithMedMonDf.filter($"${KpiConstants.memberidColName}".===("98067"))
      .select(KpiConstants.memberidColName, KpiConstants.memStartDateColName, KpiConstants.orecCodeColName, KpiConstants.lisPreSubsColName, KpiConstants.runDateColName).show()
*/




    /*create the window function based on partitioned by memebr_id and descending order of run_date*/
    val windowForStratification = Window.partitionBy(KpiConstants.memberidColName, KpiConstants.payerColName).orderBy(org.apache.spark.sql.functions.col(KpiConstants.runDateColName).desc)

    val rankColAddedDf = joinWithMedMonDf.withColumn(KpiConstants.rankColName, rank().over(windowForStratification))
                                         .filter($"${KpiConstants.rankColName}".<=(3))
                                         .withColumn(KpiConstants.lisPreSubsColName,concat(lit($"${KpiConstants.lisPreSubsColName}"), lit(":"), lit($"${KpiConstants.rankColName}")))
                                         .groupBy(KpiConstants.memberidColName, KpiConstants.payerColName)
                                         .agg(first($"${KpiConstants.lobColName}").alias(KpiConstants.lobColName),
                                              first($"${KpiConstants.lobProductColName}").alias(KpiConstants.lobProductColName),
                                              first($"${KpiConstants.memStartDateColName}").alias(KpiConstants.memStartDateColName),
                                              first($"${KpiConstants.orecCodeColName}").alias(KpiConstants.orecCodeColName),
                                              collect_list(KpiConstants.lisPreSubsColName).alias(KpiConstants.lisPreSubsColName))

    //<editor-fold desc="Last 2 months enrolled memebers">

   /* val joinLast2MonthsMemDf = rankColAddedDf.filter(($"${KpiConstants.memStartDateColName}".>=(yearNovDate))
                                                     &&($"${KpiConstants.memStartDateColName}".<=(yearEndDate)))*/

    val joinLast2MonthsMemDf = rankColAddedDf.withColumn("count",size($"${KpiConstants.lisPreSubsColName}"))
                                             .filter($"count".<(3))
                                             .drop("count")

    val rankAdded2MonthsMemDf = joinLast2MonthsMemDf.withColumn(KpiConstants.ncqaOutMeasureCol,when($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(0),lit("BCSNON"))
                                                                                              .when($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(2) || $"${KpiConstants.orecCodeColName}".cast(IntegerType).===(9),lit("BCSOT"))
                                                                                              .when($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(1) || $"${KpiConstants.orecCodeColName}".cast(IntegerType).===(3),lit("BCSDIS")))
                                                    .select(KpiConstants.memberidColName, KpiConstants.lobColName, KpiConstants.lobProductColName,KpiConstants.payerColName, KpiConstants.ncqaOutMeasureCol)
    //</editor-fold>

    //<editor-fold desc="More than 2 months enrolled members">

    val joinFullYearMemDf = rankColAddedDf.except(joinLast2MonthsMemDf)

   /* joinFullYearMemDf.printSchema()
    println("-----------------------------joinFullYearMemDf-----------------------")
    joinFullYearMemDf.filter($"${KpiConstants.memberidColName}".===("106154")).show()*/




    val lis = udf((value : Seq[String]) =>  {
      val map = value.map(f => (f.split(":")(1).toInt, f.split(":")(0).toInt)).toMap
      if((map.get(1).get == map.get(2).get == map.get(3).get) || (map.get(1).get != map.get(2).get != map.get(3).get) || (map.get(1).get == map.get(2))) map.get(1).get
      else
        map.get(2).get
    }


    )
    val lisPreSubAddedDf = joinFullYearMemDf.withColumn(KpiConstants.lisPreSubsColName, lis($"${KpiConstants.lisPreSubsColName}"))

    /*println("-----------------------------lisPreSubAddedDf-----------------------")
    lisPreSubAddedDf.filter($"${KpiConstants.memberidColName}".===("106154")).show()*/



    val measAddedFullYearDf = lisPreSubAddedDf.withColumn(KpiConstants.ncqaOutMeasureCol, when(($"${KpiConstants.lisPreSubsColName}".cast(StringType).===(""))&& ($"${KpiConstants.orecCodeColName}".cast(StringType).===("")), lit("BCSUN"))
                                                                                         .when(($"${KpiConstants.lisPreSubsColName}".cast(StringType).=!=(""))&& ($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(2) || $"${KpiConstants.orecCodeColName}".cast(IntegerType).===(9)), lit("BCSOT"))
                                                                                         .when(($"${KpiConstants.lisPreSubsColName}".cast(IntegerType).<=(0))&& ($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(0)), lit("BCSNON"))
                                                                                         .when(($"${KpiConstants.lisPreSubsColName}".cast(IntegerType).>(0))&& ($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(0)), lit("BCSLISDE"))
                                                                                         .when(($"${KpiConstants.lisPreSubsColName}".cast(IntegerType).<=(0))&& ($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(1) || $"${KpiConstants.orecCodeColName}".cast(IntegerType).===(3)), lit("BCSDIS"))
                                                                                         .when(($"${KpiConstants.lisPreSubsColName}".cast(IntegerType).>(0))&& ($"${KpiConstants.orecCodeColName}".cast(IntegerType).===(1) || $"${KpiConstants.orecCodeColName}".cast(IntegerType).===(3)), lit("BCSCMB")))
                                              .select(KpiConstants.memberidColName, KpiConstants.lobColName, KpiConstants.lobProductColName,KpiConstants.payerColName, KpiConstants.ncqaOutMeasureCol)
    //</editor-fold>

    /*Medicare members startification*/
    val medicareMemStratifiedDf = rankAdded2MonthsMemDf.union(measAddedFullYearDf)

    /*Other LOB members startification*/
    val otherMemStratifiedDf = hospiceRemMemEnrollDf.except(stratificationInputDf)
                                                    .withColumn(KpiConstants.ncqaOutMeasureCol, lit(KpiConstants.bcsMeasureId))
                                                    .select(KpiConstants.memberidColName, KpiConstants.lobColName, KpiConstants.lobProductColName,KpiConstants.payerColName, KpiConstants.ncqaOutMeasureCol)

    val startifiedTotalPopDf = medicareMemStratifiedDf.union(otherMemStratifiedDf).dropDuplicates()
    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> startifiedTotalPopDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val validVisitsOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).repartition(2).cache()
    validVisitsOutDf.coalesce(3)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir+ "/validVisitsOutDf/")

   /* validVisitsOutDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir +"/validVisitsOutDf/")*/
    //</editor-fold>

    //<editor-fold desc="Mandatory Exclusion Calculation">

    val visitForMandExclDf = spark.read.parquet(intermediateDir+ "/validVisitsOutDf/").cache()
    visitForMandExclDf.count()


    val visitForMandExclInDf = visitForMandExclDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitForMandExclInDf.count()


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
    val eligibleVisitsDf = visitForMandExclDf.except(visitForMandExclDf.filter($"${KpiConstants.memberidColName}".isin(mandatoryExcl2Df.rdd.map(r=> r.getString(0)).collect():_*)))

    eligibleVisitsDf.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(intermediateDir + "/eligibleVisitsDf/")


    val epopDf = eligibleVisitsDf.select(KpiConstants.memberidColName).distinct().cache()
    epopDf.count()
   /* epopDf.coalesce(1)
       .write
       .mode(SaveMode.Append)
       .option("header", "true")
       .csv(outDir + "/eligiblePop/")*/

    //</editor-fold>

    //<editor-fold desc="Temporary Numerator Calculation">

    val numValidVisitsDf = spark.read.parquet(intermediateDir + "/eligibleVisitsDf/").repartition(2).cache()

    numValidVisitsDf.count()

    val toutStrDf = numValidVisitsDf.select($"${KpiConstants.memberidColName}".alias(KpiConstants.ncqaOutmemberIdCol), $"${KpiConstants.ncqaOutMeasureCol}",
                                            $"${KpiConstants.payerColName}".alias(KpiConstants.ncqaOutPayerCol)).dropDuplicates()

    val numNonSuppVisitsDf =  numValidVisitsDf.filter($"${KpiConstants.supplflagColName}".===("N"))


    val nonSuppTmpNumDf = numNonSuppVisitsDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.mammographyVal))
                                                  &&($"${KpiConstants.serviceDateColName}".>=(oct1Date))
                                                  &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                            .select(KpiConstants.memberidColName)



    val otherNumTmpDf = numValidVisitsDf.except(numValidVisitsDf.filter($"${KpiConstants.memberidColName}".isin(nonSuppTmpNumDf.rdd.map(f=> f.getString(0)).collect():_*)))
                                        .filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.mammographyVal))
                                              &&($"${KpiConstants.serviceDateColName}".>=(oct1Date))
                                              &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                        .select(KpiConstants.memberidColName)


    val numTmpDf = nonSuppTmpNumDf.union(otherNumTmpDf)
   /* numTmpDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(outDir + "/numTmpDf/")*/


    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val optionalExclVisistsDf = numValidVisitsDf.except(numValidVisitsDf.filter($"${KpiConstants.memberidColName}".isin(numTmpDf.rdd.map(f=> f.getString(0)).collect():_*)))
                                                .repartition(2).cache()
    optionalExclVisistsDf.count()



    //<editor-fold desc="Optional Exclusion Non supplement Calculation">

    val optExclNonSupVisDf =  optionalExclVisistsDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    optExclNonSupVisDf.count()

    //<editor-fold desc="First Optional Exclsuion">

    val firstOptExclNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralMastectomyVal))
                                                       &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                       &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                 .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Second Optional Exclsuion">

    val secondOptExclNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                        &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                  .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Third Optional Exclsuion">

    val thirdOptExclNonSupInDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                         &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                         &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                         &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                         &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                         &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                   .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val thirdOptExclNonSupDf = thirdOptExclNonSupInDf.as("df1").join(thirdOptExclNonSupInDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                                      .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                                      .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Fourth Optional Exclsuion">

    val fourOptExclNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.historyBilateralMastectomyVal))
                                                      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Fifth Optional Exclsuion">

    val fifthOptExclNonSupIn1Df = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                          &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                          &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                    .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthOptExclNonSupIn2Df = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                          &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                          &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                          &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                    .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthOptExclNonSupIn3Df =  optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyLeftVal))
                                                           &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                           &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                     .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthunion2And3NonSupDf = fifthOptExclNonSupIn2Df.union(fifthOptExclNonSupIn3Df)

    val fifthOptExclNonSupDf = fifthOptExclNonSupIn1Df.as("df1").join(fifthunion2And3NonSupDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                      .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                      .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="Sixth Optional Exclsuion">

    val sixthOptExclNonSupIn1Df = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                          &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                          &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                          &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                    .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthOptExclNonSupIn2Df = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                          &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                          &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                          &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                    .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthOptExclNonSupIn3Df =  optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyRightVal))
                                                           &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                           &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                     .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthunion2And3NonSupDf = sixthOptExclNonSupIn2Df.union(sixthOptExclNonSupIn3Df)

    val sixthOptExclNonSupDf = sixthOptExclNonSupIn1Df.as("df1").join(sixthunion2And3NonSupDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                                       .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                                       .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="seventh Optional Exclusion">

    val seventhaNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                   &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventhbNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.absOfLeftBreastVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventhcNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyLeftVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventhNonSup1Df = seventhaNonSupDf.union(seventhbNonSupDf).union(seventhcNonSupDf)


    val seventhdNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                   &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventheNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.absOfRightBreastVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventhfNonSupDf = optExclNonSupVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyRightVal))
                                                   &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                             .select(KpiConstants.memberidColName)

    val seventhNonSup2Df = seventhdNonSupDf.union(seventheNonSupDf).union(seventhfNonSupDf)

    val seventhOptExclNonSupDf = seventhNonSup1Df.intersect(seventhNonSup2Df)
    //</editor-fold>


    val nonSupOptExclDf = firstOptExclNonSupDf.union(secondOptExclNonSupDf).union(thirdOptExclNonSupDf).union(fourOptExclNonSupDf)
      .union(fifthOptExclNonSupDf).union(sixthOptExclNonSupDf).union(seventhOptExclNonSupDf)

    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Other Calculation">

    val optExclOtherVisDf =  optionalExclVisistsDf.except(optionalExclVisistsDf.filter($"${KpiConstants.memberidColName}".isin(nonSupOptExclDf.rdd.map(f => f.getString(0)).collect():_*)))
    optExclOtherVisDf.count()


    //<editor-fold desc="First Optional Exclsuion">

    val firstOptExclOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralMastectomyVal))
                                                     &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                     &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                               .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Second Optional Exclsuion">

    val secondOptExclOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                      &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                      &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Third Optional Exclsuion">

    val thirdOptExclOtherInDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                       &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                       &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                       &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                       &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                       &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                 .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val thirdOptExclOtherDf = thirdOptExclOtherInDf.as("df1").join(thirdOptExclOtherInDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                                    .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                                    .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Fourth Optional Exclsuion">

    val fourOptExclOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.historyBilateralMastectomyVal))
                                                    &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                    &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                              .select(KpiConstants.memberidColName)
    //</editor-fold>

    //<editor-fold desc="Fifth Optional Exclsuion">

    val fifthOptExclOtherIn1Df = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                 .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthOptExclOtherIn2Df = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                        &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                  .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthOptExclOtherIn3Df =  optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyLeftVal))
                                                         &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                         &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                   .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val fifthunion2And3OtherDf = fifthOptExclOtherIn2Df.union(fifthOptExclOtherIn3Df)

    val fifthOptExclOtherDf = fifthOptExclOtherIn1Df.as("df1").join(fifthunion2And3OtherDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                    .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                    .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="Sixth Optional Exclsuion">

    val sixthOptExclOtherIn1Df = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                        &&(!array_contains($"${KpiConstants.valuesetColName}", KpiConstants.bilateralModifierVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                  .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthOptExclOtherIn2Df = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                        &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                  .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthOptExclOtherIn3Df =  optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyRightVal))
                                                         &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                         &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                                   .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName)

    val sixthunion2And3OtherDf = sixthOptExclOtherIn2Df.union(sixthOptExclOtherIn3Df)

    val sixthOptExclOtherDf = sixthOptExclOtherIn1Df.as("df1").join(sixthunion2And3OtherDf.as("df2"), Seq(KpiConstants.memberidColName), KpiConstants.innerJoinType)
                                                    .filter(abs(datediff($"df2.${KpiConstants.serviceDateColName}",$"df1.${KpiConstants.serviceDateColName}")).>=(14))
                                                    .select(KpiConstants.memberidColName)

    //</editor-fold>

    //<editor-fold desc="seventh Optional Exclusion">

    val seventhaOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                 &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.leftModifierVal))
                                                 &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                 &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                            .select(KpiConstants.memberidColName)

    val seventhbOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.absOfLeftBreastVal))
                                                 &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                 &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                           .select(KpiConstants.memberidColName)

    val seventhcOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyLeftVal))
                                                 &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                 &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                          .select(KpiConstants.memberidColName)

    val seventhOther1Df = seventhaOtherDf.union(seventhbOtherDf).union(seventhcOtherDf)


    val seventhdOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyVal))
                                                 &&(array_contains($"${KpiConstants.valuesetColName}", KpiConstants.rightModifierVal))
                                                 &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                 &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                           .select(KpiConstants.memberidColName)

    val seventheOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.absOfRightBreastVal))
                                                  &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                           .select(KpiConstants.memberidColName)

    val seventhfOtherDf = optExclOtherVisDf.filter((array_contains($"${KpiConstants.valuesetColName}", KpiConstants.unilateralMastectomyRightVal))
                                                 &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                 &&($"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
                                          .select(KpiConstants.memberidColName)

    val seventhOther2Df = seventhdOtherDf.union(seventheOtherDf).union(seventhfOtherDf)

    val seventhOptExclOtherDf = seventhOther1Df.intersect(seventhOther2Df)
    //</editor-fold>


    val otherOptExclDf = firstOptExclOtherDf.union(secondOptExclOtherDf).union(thirdOptExclOtherDf).union(fourOptExclOtherDf)
      .union(fifthOptExclOtherDf).union(sixthOptExclOtherDf).union(seventhOptExclOtherDf)

    //</editor-fold>

    val optinalExclDf = nonSupOptExclDf.union(otherOptExclDf)


    //</editor-fold>

      /*Numerator Calculation*/
    val numeratorDf = numTmpDf.except(optinalExclDf)


    //<editor-fold desc="Ncqa Output Creation">

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutStrDf, KpiConstants.eligibleDfName -> epopDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> optinalExclDf,
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
