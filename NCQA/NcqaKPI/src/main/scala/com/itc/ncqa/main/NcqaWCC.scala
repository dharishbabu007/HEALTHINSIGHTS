package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object NcqaWCC {


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

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val aLiat = List("col1")
    val msrVal = s"'$measureId'"
    val ggMsrId = s"'${KpiConstants.ggMeasureId}'"
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName, KpiConstants.marketplaceLobName)
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

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Age filter">


    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageFilterDf = membershipDf.withColumn("age",datediff(lit(ageEndDate),$"${KpiConstants.dateofbirthColName}")/365.25).filter($"age">=3 and $"age"<18)

   /* println("ageFilterDf")
    ageFilterDf.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap Calculation">

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))
    // val argMapForContEnrollFunction = mutable.Map(KpiConstants.ageStartKeyName -> "12", )
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName,UtilFunctions.add_ncqa_months(spark,lit(ageStartDate), 0))
      .withColumn(KpiConstants.contenrollUppCoName,UtilFunctions.add_ncqa_months(spark,lit(ageEndDate), 0))
      .withColumn(KpiConstants.anchorDateColName, UtilFunctions.add_ncqa_months(spark,lit(ageEndDate), 0))

    /* println("contEnrollInDf")
     contEnrollInDf.filter($"${KpiConstants.memberidColName}" === ("117463")).show()*/


    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      ||($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollLowCoName}") && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}"))))
      .withColumn(KpiConstants.anchorflagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}")), lit(1)).otherwise(lit(0)))
      .withColumn(KpiConstants.contEdFlagColName, when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollUppCoName}")), lit(1)).otherwise(lit(0)))

    /* println("contEnrollStep1Df")
     contEnrollStep1Df.filter($"${KpiConstants.memberidColName}" === ("117463")).show()*/


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

    /* printf("contEnrollStep2Df")
     contEnrollStep2Df.show()*/

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


    /* println("contEnrollStep3Df")
     contEnrollStep3Df.filter($"${KpiConstants.memberidColName}" === ("117463")).show()*/



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


    /* printf("contEnrollmemDf")
     contEnrollmemDf.show()*/

    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}",s"df1.${KpiConstants.primaryPlanFlagColName}").cache()

    /* printf("contEnrollDf")
     contEnrollDf.show()*/

    //</editor-fold>

    //<editor-fold desc="Dual Enrollment and W15 Lob filter">

    val baseOutDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList,measureId)

    /*printf("baseOutDf")
    baseOutDf.show()*/

  /*  println("baseOutDf")
    baseOutDf.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/

    val w15ContEnrollDf = baseOutDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*)).dropDuplicates().cache()
    w15ContEnrollDf.count()

   /* w15ContEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/w15ContEnrollDf/")*/

    //</editor-fold>

    //<editor-fold desc="Initial Join with Ref_Hedis">

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.bmiPercentileVal,KpiConstants.outPatientVal,KpiConstants.nutritionCounselVal,KpiConstants.physicalActCounselVal,KpiConstants.pregnancyVal)


    val medList = KpiConstants.emptyList
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medList)
    /*println("visitRefHedisDf")
    visitRefHedisDf.filter($"${KpiConstants.memberidColName}" === ("107650")).show()*/

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
        KpiConstants.dobColName,KpiConstants.supplflagColName, KpiConstants.ispcpColName,KpiConstants.isobgynColName, KpiConstants.valuesetColName,KpiConstants.genderColName)

    /*visitgroupedDf.filter($"${KpiConstants.memberidColName}" === ("107650")).show()*/

    val indLabVisRemDf = visitgroupedDf.filter(!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)).repartition(2).cache()
    indLabVisRemDf.count()
   /* println("indLabVisRemDf")
    indLabVisRemDf.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/
    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .dropDuplicates()

    hospiceInCurrYearMemDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w34/hospiceInCurrYearMemDf/")

    //</editor-fold>

    val totalPopOutDf = w15ContEnrollDf.except(w15ContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(hospiceInCurrYearMemDf.rdd.map(r=>r.getString(0)).collect():_*)))

  /*  println("totalPopOutDf")
    totalPopOutDf.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/

   /* totalPopOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .csv("/home/hbase/ncqa/wcc/totalPopOutDf/")*/
    /* val totalPopOutDf = w15ContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(totalPopmemidDf:_*))
       .select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.payerColName)
       .dropDuplicates()*/

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> totalPopOutDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val visitJoinedOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin)
    visitJoinedOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("/home/hbase/ncqa/wcc/visitJoinedDf/")

   /* println("visitJoinedOutDf")
    visitJoinedOutDf.filter($"${KpiConstants.memberidColName}" === ("105720")).show()*/

    val visitJoinedeforeventDf = spark.read.parquet("/home/hbase/ncqa/wcc/visitJoinedDf/").repartition(2).cache()


    val visiteventout=visitJoinedeforeventDf.withColumn("age",datediff(lit(ageEndDate),$"${KpiConstants.dobColName}")/365.25).filter(array_contains($"${KpiConstants.valuesetColName}",KpiConstants.outPatientVal)
      &&($"${KpiConstants.ispcpColName}".===("Y") || $"${KpiConstants.isobgynColName}".===("Y")) && ($"${KpiConstants.supplflagColName}".===("N"))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
  /*  println("visiteventout_schema")
    visiteventout.printSchema()*/
    val EpopDf=visiteventout.select(KpiConstants.memberidColName).dropDuplicates()
    EpopDf.cache()
    val touttemp =visiteventout.
      select($"${KpiConstants.memberidColName}",$"${KpiConstants.memberidColName}".as(KpiConstants.ncqaOutmemberIdCol),$"${KpiConstants.payerColName}".as(KpiConstants.ncqaOutPayerCol),$"age").distinct()
    val touttempcls= touttemp.withColumn("clasfication",when($"age">=3 and $"age"<12,lit("CH1")).when($"age">=12 and $"age"<18,lit("CH2")))

    val statification = Seq(("CH1", "WCC1A"),("CH2", "WCC2A"),("CH1", "WCC1B"),("CH2", "WCC2B"),("CH1", "WCC1C"),("CH2", "WCC2C")).toDF("clasfication", "meas")
    val toutStrDf=touttempcls.as("df1").join(statification.as("df2"), $"df1.clasfication"===$"df2.clasfication", KpiConstants.innerJoinType).cache()
      .select("df1.*","df2.meas")

   /* println("toutStrDf")
    toutStrDf.printSchema()
    toutStrDf.show()
    println("EpopDf")*/
    // EpopDf.filter($"${KpiConstants.memberidColName}" === ("96366")).show()
   /* EpopDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .csv("/home/hbase/ncqa/wcc/EpopDf/")*/
    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val wccinputNumerator = visitJoinedeforeventDf.as("df1").join(EpopDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType).cache()
      .select("df1.*")
    val wccnum1nonsup= wccinputNumerator.filter(($"${KpiConstants.supplflagColName}".===("N")) && (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiPercentileVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)
    val wccnum2nonsup= wccinputNumerator.filter(($"${KpiConstants.supplflagColName}".===("N")) && (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nutritionCounselVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)
    val wccnum3nonsup= wccinputNumerator.filter(($"${KpiConstants.supplflagColName}".===("N")) && (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.physicalActCounselVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)

    val wccinputsup1 = wccinputNumerator.except(wccinputNumerator.filter($"${KpiConstants.memberidColName}".isin(
      wccnum1nonsup.rdd.map(r=>r.getString(0)).collect():_*)))
    val wccinputsup2 = wccinputNumerator.except(wccinputNumerator.filter($"${KpiConstants.memberidColName}".isin(
      wccnum2nonsup.rdd.map(r=>r.getString(0)).collect():_*)))
    val wccinputsup3 = wccinputNumerator.except(wccinputNumerator.filter($"${KpiConstants.memberidColName}".isin(
      wccnum3nonsup.rdd.map(r=>r.getString(0)).collect():_*)))

    val wccnum1sup= wccinputsup1.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.bmiPercentileVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)
    val wccnum2sup= wccinputsup2.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.nutritionCounselVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)
    val wccnum3sup= wccinputsup3.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.physicalActCounselVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)


    val wccnum1=wccnum1nonsup.union(wccnum1sup)
    val wccnum2=wccnum2nonsup.union(wccnum2sup)
    val wccnum3=wccnum3nonsup.union(wccnum3sup)

  /*  println("wccnum1")
    wccnum1.filter($"${KpiConstants.memberidColName}" === ("105720")).show()

    println("wccnum2")
    wccnum2.filter($"${KpiConstants.memberidColName}" === ("105720")).show()

    println("wccnum3")
    wccnum3.filter($"${KpiConstants.memberidColName}" === ("105720")).show()
*/

    //   val  TotalNumertor= wccnum1.union(wccnum2).union(wccnum3)
    val  TotalNumertor= wccnum1.intersect(wccnum2).intersect(wccnum3)
    val excludeepop = EpopDf.except(EpopDf.filter($"${KpiConstants.memberidColName}".isin(
      TotalNumertor.rdd.map(r=>r.getString(0)).collect():_*)))

    val wccoptionalexlinput = visitJoinedeforeventDf.as("df1").join(excludeepop.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select("df1.*")

    val wccoptexlnonsup= wccoptionalexlinput.filter(($"${KpiConstants.genderColName}".===("F")) && ($"${KpiConstants.supplflagColName}".===("N")) && (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.pregnancyVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)

    val wccoptexlsupinput = wccoptionalexlinput.except(wccoptionalexlinput.filter($"${KpiConstants.memberidColName}".isin(
      wccoptexlnonsup.rdd.map(r=>r.getString(0)).collect():_*)))

    val wccoptexlsup= wccoptexlsupinput.filter(($"${KpiConstants.genderColName}".===("F")) && (array_contains($"${KpiConstants.valuesetColName}",KpiConstants.pregnancyVal)) && ($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate))).select(KpiConstants.memberidColName)
    val wccoptexl=wccoptexlnonsup.union(wccoptexlsup)


    val Num1 = wccnum1.except(wccnum1.filter($"${KpiConstants.memberidColName}".isin(
      wccoptexl.rdd.map(r=>r.getString(0)).collect():_*)))


    val Num2 = wccnum2.except(wccnum2.filter($"${KpiConstants.memberidColName}".isin(
      wccoptexl.rdd.map(r=>r.getString(0)).collect():_*)))


    val Num3 = wccnum3.except(wccnum3.filter($"${KpiConstants.memberidColName}".isin(
      wccoptexl.rdd.map(r=>r.getString(0)).collect():_*)))
    //</editor-fold>

    //<editor-fold desc="Ncqa Output Creation">

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutStrDf, KpiConstants.eligibleDfName -> EpopDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> wccoptexl,
      KpiConstants.numeratorDfName -> Num1, KpiConstants.numerator2DfName -> Num2, KpiConstants.numerator3DfName -> Num3
    )

    val outDf = UtilFunctions.ncqaWCCOutputDfCreation(spark,outMap)
    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/wcc/outDf/")
    //</editor-fold>

    spark.sparkContext.stop()

  }
}
