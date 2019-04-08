package com.itc.ncqa.main

import java.sql.Date

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConversions._
import scala.collection.mutable


case class Member(member_id:String, service_date:Date)
case class GroupMember(member_id:String, dateList:List[Long])
object NcqaW15 {

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

    //<editor-fold desc="Age filter">

    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageFilterDf = membershipDf.filter(date_add(add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months12),90).>=(ageStartDate) && date_add(add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months12),90).<=(ageEndDate))

    /*println("ageFilterDf")
    ageFilterDf.filter($"${KpiConstants.memberidColName}" === ("117463")).show()*/

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap Calculation">

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))
    // val argMapForContEnrollFunction = mutable.Map(KpiConstants.ageStartKeyName -> "12", )
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName,date_add($"${KpiConstants.dateofbirthColName}",31))
      .withColumn(KpiConstants.contenrollUppCoName,date_add(add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months12),90))
      .withColumn(KpiConstants.anchorDateColName, date_add(add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months12),90))

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

    val w15ContEnrollDf = baseOutDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*)).dropDuplicates().cache()
    w15ContEnrollDf.count()

    w15ContEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/w15ContEnrollDf/")
    //</editor-fold>

    //<editor-fold desc="Initial Join with Ref_Hedis">

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.wellCareVal)


    val medList = KpiConstants.emptyList
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medList)
    /*println("visitRefHedisDf")
    visitRefHedisDf.filter($"${KpiConstants.memberidColName}" === ("107650")).show()*/

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
        KpiConstants.dobColName,KpiConstants.supplflagColName, KpiConstants.ispcpColName,KpiConstants.isobgynColName, KpiConstants.valuesetColName)

    /*visitgroupedDf.filter($"${KpiConstants.memberidColName}" === ("107650")).show()*/

    val indLabVisRemDf = visitgroupedDf.filter(!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)).repartition(2).cache()
    indLabVisRemDf.count()
   /* println("indLabVisRemDf")
    indLabVisRemDf.filter($"${KpiConstants.memberidColName}" === ("107650")).show()*/
    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
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

    val totalPopmemidDf = w15ContEnrollDf.select(KpiConstants.memberidColName).except(hospiceInCurrYearMemDf).distinct()
      .rdd
      .map(r=> r.getString(0))
      .collect()

    val totalPopOutDf = w15ContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(totalPopmemidDf:_*))
                                       .select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.payerColName)
                                       .dropDuplicates()


    val msrList = List(KpiConstants.w150MeasureId, KpiConstants.w151MeasureId, KpiConstants.w152MeasureId,
                       KpiConstants.w153MeasureId, KpiConstants.w154MeasureId, KpiConstants.w155MeasureId,
                       KpiConstants.w156MeasureId)

    val lobNameList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName)
    val remMsrlist = List(KpiConstants.w150MeasureId, KpiConstants.w151MeasureId, KpiConstants.w152MeasureId,
                          KpiConstants.w153MeasureId, KpiConstants.w154MeasureId, KpiConstants.w155MeasureId)

    val toutStrDf = UtilFunctions.toutOutputCreation(spark,totalPopOutDf,msrList,lobNameList,remMsrlist)
                                 .select(KpiConstants.memberidColName, KpiConstants.ncqaOutPayerCol, KpiConstants.ncqaOutMeasureCol)



    //val toutStrDf =totalPopOutDf.select($"${KpiConstants.memberidColName}",$"${KpiConstants.memberidColName}".as(KpiConstants.ncqaOutmemberIdCol),$"${KpiConstants.payerColName}".as(KpiConstants.ncqaOutPayerCol))

    toutStrDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("/home/hbase/ncqa/w15/tout/")

    val eligiblePopDf = toutStrDf.select(KpiConstants.memberidColName).distinct().repartition(2).cache()
    eligiblePopDf.count()
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val denominatorDf = eligiblePopDf
    denominatorDf.cache()
    denominatorDf.count()

  /*  denominatorDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/denominator/")
*/
    //</editor-fold>

    //<editor-fold desc="Intial join">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> denominatorDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val visitJoinedOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin)
    visitJoinedOutDf.count()
    visitJoinedOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("/home/hbase/ncqa/w15/visitJoinedDf/")
    //</editor-fold>

    //<editor-fold desc="W15 Numerator Calculation">

    val visitJoinedDf = spark.read.parquet("/home/hbase/ncqa/w15/visitJoinedDf/").repartition(2).cache()
    visitJoinedDf.count()
    val toutDf = spark.read.parquet("/home/hbase/ncqa/w15/tout/")
    val denominatorPopDf = toutDf.select(KpiConstants.memberidColName).distinct().cache()




    val w15NumDf = visitJoinedDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.wellCareVal))
                                      &&($"${KpiConstants.ispcpColName}".===("Y"))
                                      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                      &&($"${KpiConstants.serviceDateColName}".<= (date_add(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months12),90))))
                                .select(s"${KpiConstants.memberidColName}",s"${KpiConstants.serviceDateColName}")
    /*First numerator*/
    val W150 = denominatorPopDf.except(w15NumDf.select(s"${KpiConstants.memberidColName}").distinct())

    /*Second Numerator*/
    val W151_1 = w15NumDf.groupBy($"${KpiConstants.memberidColName}")
      .agg(countDistinct($"${KpiConstants.serviceDateColName}").alias("visits"))
      .filter($"visits".===(1))
      .select($"${KpiConstants.memberidColName}").distinct()


    /*W15 Numerator 2 or more visits*/
    val w152OrMoreVisitsDf = w15NumDf.except(w15NumDf.filter($"${KpiConstants.memberidColName}".isin(W151_1.rdd.map(r=> r.getString(0)).collect():_*)))

    val groupedDs = w152OrMoreVisitsDf.as[Member].groupByKey(k=>k.member_id)
                                      .mapGroups((k,itr) => (k,itr.map(f=> f.service_date.getTime).toArray.sorted))

    val w152OrMoreNumDf = groupedDs.map(f=>(UtilFunctions.getVisits(f._1,f._2))).toDF(KpiConstants.memberidColName,"visits")

    val W151_2 = w152OrMoreNumDf.filter($"visits".===(1)).select(KpiConstants.memberidColName)
    val W151 = W151_1.union(W151_2)
    val W152 = w152OrMoreNumDf.filter($"visits".===(2)).select(KpiConstants.memberidColName)
    val W153 = w152OrMoreNumDf.filter($"visits".===(3)).select(KpiConstants.memberidColName)
    val W154 = w152OrMoreNumDf.filter($"visits".===(4)).select(KpiConstants.memberidColName)
    val W155 = w152OrMoreNumDf.filter($"visits".===(5)).select(KpiConstants.memberidColName)
    val W156 = w152OrMoreNumDf.filter($"visits".>=(6)).select(KpiConstants.memberidColName)


   /* W150.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W150/")
    W151.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W151/")
    W152.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W152/")
    W153.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W153/")
    W154.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W154/")
    W155.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W155/")
    W156.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/W156/")
*/
    //</editor-fold>


    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutDf, KpiConstants.eligibleDfName -> denominatorPopDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> spark.emptyDataFrame,
      KpiConstants.numeratorDfName -> W150, KpiConstants.numerator2DfName -> W151, KpiConstants.numerator3DfName -> W152,
      KpiConstants.numerator4DfName -> W153, KpiConstants.numerator5DfName -> W154, KpiConstants.numerator6DfName -> W155,
      KpiConstants.numerator7DfName -> W156)

    val outDf = UtilFunctions.ncqaW15OutputDfCreation(spark,outMap)
    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/w15/outDf/")


      spark.sparkContext.stop()
    }

}
