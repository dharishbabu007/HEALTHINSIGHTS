package com.itc.ncqa.main

import java.sql.Date

import com.itc.ncqa.Constants
import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
//import com.itc.ncqa.Functions.SparkObject._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.mutable

//case class Member(member_id:String, service_date:Date)
//case class GroupMember(member_id:String, dateList:List[Long])

object NcqaIMA {


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

    val aLiat = List("col1")
    val msrVal = "'"+measureId+"'"
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
    val memqueryString = "SELECT * FROM "+ KpiConstants.dbName +".membership_enrollment WHERE measure = "+msrVal+" and  (member_plan_start_date IS  NOT NULL) AND(member_plan_end_date IS NOT NULL)"
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,memqueryString,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))
                                        .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")

    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)
    val visitqueryString = "SELECT * FROM "+ KpiConstants.dbName +".visits WHERE measure = "+ msrVal+" and  (service_date IS  NOT NULL) AND((admit_date IS NULL and discharge_date IS NULL) OR (ADMIT_DATE IS NOT NULL AND DISCHARGE_DATE IS NOT NULL))"
    val visitsDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,visitqueryString,aLiat)
                                    .filter((($"${KpiConstants.dataSourceColName}".===("Claim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
                                          || (($"${KpiConstants.dataSourceColName}".===("RxClaim"))&&($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))
                                          ||($"${KpiConstants.dataSourceColName}".===("Rx")))
                                    .drop(KpiConstants.lobProductColName, "latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name","product")
      .withColumn(KpiConstants.revenuecodeColName, when((length($"${KpiConstants.revenuecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.revenuecodeColName}"))).otherwise($"${KpiConstants.revenuecodeColName}"))
      .withColumn(KpiConstants.billtypecodeColName, when((length($"${KpiConstants.billtypecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.billtypecodeColName}"))).otherwise($"${KpiConstants.billtypecodeColName}"))
      .withColumn(KpiConstants.proccode2ColName, when(($"${KpiConstants.proccode2mod1ColName}".isin(KpiConstants.avoidCodeList:_*)) || ($"${KpiConstants.proccode2mod2ColName}".isin(KpiConstants.avoidCodeList:_*)),lit("NA")).otherwise($"${KpiConstants.proccode2ColName}"))


   /* val medmonmemDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,KpiConstants.imaMeasureId,aLiat)
                                       .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
*/
   /* val productPlanDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.productPlanTblName,KpiConstants.imaMeasureId,aLiat)
                                         .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
*/


    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter(($"${KpiConstants.measureIdColName}".===(KpiConstants.imaMeasureId)) || ($"${KpiConstants.measureIdColName}".===(KpiConstants.ggMeasureId)))
                                      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


    broadcast(refHedisDf)


    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.imaMeasureId))
                                             .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")


    broadcast(ref_medvaluesetDf)

    //</editor-fold

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Age filter">

    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months156).>=(ageStartDate)) && (UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",KpiConstants.months156).<=(ageEndDate)))

    /*  ageFilterDf.select(KpiConstants.memberidColName).coalesce(1)
        .write
        .mode(SaveMode.Append)
        .option("header", "true")
        .csv("/home/hbase/ncqa/ima_test_out/age/")*/
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName,UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months144))
      .withColumn(KpiConstants.contenrollUppCoName,UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months156))
      .withColumn(KpiConstants.anchorDateColName, UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}", KpiConstants.months156))



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



    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}",s"df1.${KpiConstants.primaryPlanFlagColName}").cache()

    // contEnrollDf.show()
     /*contEnrollDf.select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.primaryPlanFlagColName).dropDuplicates()
       .coalesce(1)
       .write
       .mode(SaveMode.Append)
       .option("header", "true")
       .csv("/home/hbase/ncqa/ima_test_out/contEnrolldetail/")*/


    //</editor-fold>

    //<editor-fold desc="Dual eligibility,Dual enrollment, Ima enrollment filter">

    val baseOutDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList,measureId)
    //baseOutDf.show()
     /* baseOutDf.coalesce(1)
        .write
        .mode(SaveMode.Append)
        .option("header", "true")
        .csv("/home/hbase/ncqa/ima_test_out/baseOut/")*/

    val imaContEnrollDf = baseOutDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*)).dropDuplicates().cache()
     imaContEnrollDf.count()
    //imaContEnrollDf.show()
    /* imaContEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
       .write
       .mode(SaveMode.Append)
       .option("header", "true")
       .csv("/home/hbase/ncqa/ima_test_out/imaContEnrollDf/")*/
    //</editor-fold>

    //<editor-fold desc="Initial Join with Ref_Hedis">

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.meningococcalVal, KpiConstants.tdapVacceVal,
      KpiConstants.hpvVal, KpiConstants.ardvVal, KpiConstants.ardtsVal, KpiConstants.encephalopathyVal, KpiConstants.vaccineAdverseVal)


    val medList = KpiConstants.emptyList
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medList)
    //visitRefHedisDf.show()
    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
                                        .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
                                                KpiConstants.dobColName,KpiConstants.supplflagColName, KpiConstants.valuesetColName)

    val indLabVisRemDf = visitgroupedDf.filter(!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)).repartition(90).cache()
    indLabVisRemDf.count()

    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val yearStartDate = year+"-01-01"
    val yearEndDate = year+"-12-31"
    val hospiceInCurrYearMemDf = indLabVisRemDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.hospiceVal))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(KpiConstants.memberidColName)
      .dropDuplicates()

   /* hospiceInCurrYearMemDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/hospiceInCurrYearMemDf/")*/

    //</editor-fold>



    val totalPopmemidDf = imaContEnrollDf.select(KpiConstants.memberidColName).except(hospiceInCurrYearMemDf).distinct()
      .rdd
      .map(r=> r.getString(0))
      .collect()

    //val eligibleMemDf = hospiceRemovedMemsDf.select(KpiConstants.memberidColName).distinct().intersect(validVisitsDf.select(KpiConstants.memberidColName)).dropDuplicates()
    val totalPopOutDf = imaContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(totalPopmemidDf:_*))
      .select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.payerColName).dropDuplicates()

    val msrList = List(KpiConstants.imahpvMeasureId, KpiConstants.imamenMeasureId, KpiConstants.imatdMeasureId,
                       KpiConstants.imacmb1MeasureId, KpiConstants.imacmb2MeasureId)

    val lobNameList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName)
    val remMsrlist = List(KpiConstants.imacmb1MeasureId)
    val toutStrDf = UtilFunctions.toutOutputCreation(spark,totalPopOutDf,msrList,lobNameList,remMsrlist)
                                 .select(KpiConstants.memberidColName, KpiConstants.ncqaOutPayerCol, KpiConstants.ncqaOutMeasureCol)


    toutStrDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("/home/hbase/ncqa/ima_test_out/tout/")

    val eligiblePopDf = totalPopOutDf.select(KpiConstants.memberidColName).distinct().repartition(90).cache()
    eligiblePopDf.count()
    //</editor-fold>

    //<editor-fold desc="Dinominator calculation">

    val denominatorDf = eligiblePopDf
    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> denominatorDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val visitJoinedOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin)
    visitJoinedOutDf.count()
    visitJoinedOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("/home/hbase/ncqa/ima_test_out/visitJoinedDf/")


    imaContEnrollDf.unpersist()
    indLabVisRemDf.unpersist()
    eligiblePopDf.unpersist()
    denominatorDf.unpersist()
    //</editor-fold>

    //<editor-fold desc="IMA tmp Numerator Calculation">

    val visitJoinedDf = spark.read.parquet("/home/hbase/ncqa/ima_test_out/visitJoinedDf/").repartition(20).cache()
    visitJoinedDf.count()
    val visitNonSuppDf = visitJoinedDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitNonSuppDf.count()
    val toutDf = spark.read.parquet("/home/hbase/ncqa/ima_test_out/tout/")
    val denominatorPopDf = toutDf.select(KpiConstants.memberidColName).distinct().cache()



    //<editor-fold desc="IMA Menucocal Tmp Numerator Calculation">

    val imaMenValueSet = List(KpiConstants.meningococcalVal)
    val imaMenNonSuppDf = visitNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.meningococcalVal))
      && ($"${KpiConstants.serviceDateColName}".>=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months132)))
      && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months156))))
      .select(s"${KpiConstants.memberidColName}").cache()

    val imaMenOtherMemIdDf = denominatorPopDf.except(imaMenNonSuppDf)

    val visitForMenOtherDf = visitJoinedDf.as("df1").join(imaMenOtherMemIdDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")


    val imaMenOtherDf = visitForMenOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.meningococcalVal))
      && ($"${KpiConstants.serviceDateColName}".>=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months132)))
      && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months156))))
      .select(s"${KpiConstants.memberidColName}")

    val imaMenTmpDf = imaMenNonSuppDf.union(imaMenOtherDf).dropDuplicates().cache()

    /*imaMenTmpDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaMenTmpDf/")*/

    //</editor-fold>

    //<editor-fold desc="IMA HPV Numerator Non Supplement Calculation">

    val imaHpvNonSuppDf = UtilFunctions.imaHpvNumeratorCalculation(spark,visitNonSuppDf)

    val imaHpvOtherMemIdDf = denominatorPopDf.except(imaHpvNonSuppDf)

    val visitForHpvOtherDf = visitJoinedDf.as("df1").join(imaHpvOtherMemIdDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")


    val imaHpvOtherDf = UtilFunctions.imaHpvNumeratorCalculation(spark,visitForHpvOtherDf)

    val imaHpvTmpDf = imaHpvNonSuppDf.union(imaHpvOtherDf).cache()

    imaHpvTmpDf.count()

   /* imaHpvTmpDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaHpvTmpDf/")*/

    //</editor-fold>

    //<editor-fold desc="IMA Tdap Numerator Non Supplement Calculation">

    val imaTdapValueSet = List(KpiConstants.tdapVacceVal)

    val imaTdapNonSuppDf = visitNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.tdapVacceVal ))
      && ($"${KpiConstants.serviceDateColName}".>=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months120)))
      && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months156))))
      .select(s"${KpiConstants.memberidColName}")


    val imaTdapOtherMemDf = denominatorPopDf.except(imaTdapNonSuppDf)

    val visitForTdapOtherDf = visitJoinedDf.as("df1").join(imaTdapOtherMemDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")

    val imaTdapOtherDf = visitForTdapOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.tdapVacceVal ))
      && ($"${KpiConstants.serviceDateColName}".>=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months120)))
      && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}", KpiConstants.months156))))
      .select(s"${KpiConstants.memberidColName}")

    val imaTdapTmpDf = imaTdapNonSuppDf.union(imaTdapOtherDf).cache()
    imaTdapTmpDf.count()

   /* imaTdapTmpDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaTdapTmpDf/")*/

    //</editor-fold>

    //</editor-fold>

    //<editor-fold desc="Optional Exclusion Calculation">

    val eligMemForOptDf = denominatorPopDf.except(imaMenTmpDf.intersect(imaHpvTmpDf).intersect(imaTdapTmpDf)).cache()

    val visitForOptDf = visitJoinedDf.as("df1").join(eligMemForOptDf.as("df2"), KpiConstants.memberidColName)
                                      .select("df1.*").cache()
    visitForOptDf.count()

    /*First Optinal exclusion on Non Supplement data*/

    val visitNonSuppOptDf = visitForOptDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitNonSuppOptDf.count()

    //<editor-fold desc="ARDV on or before 13th birth day">

    val imaDinoExclValSet1 = List(KpiConstants.ardvVal)
    val ardvBef13yearNonSuppdf = visitNonSuppOptDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.ardvVal))
                                                        &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                        &&($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months156))))
                                                  .select(s"${KpiConstants.memberidColName}").distinct()

    val ardvOtherMemDf = eligMemForOptDf.except(ardvBef13yearNonSuppdf).rdd
      .map(r=> r.getString(0))
      .collect()

    val visitArdvOtherDf = visitForOptDf.filter($"${KpiConstants.memberidColName}".isin(ardvOtherMemDf:_*))

    val ardvBef13yearOtherdf = visitArdvOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.ardvVal))
                                                      &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                      &&($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months156))))
                                                .select(s"${KpiConstants.memberidColName}").distinct()
    val ardvBef13Df = ardvBef13yearNonSuppdf.union(ardvBef13yearOtherdf).cache()
    ardvBef13Df.count()

     /* ardvBef13Df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/ardvBef13Df/")
*/
    //</editor-fold>

    //<editor-fold desc="Anaphylactic Reaction Due To Serum Value Set">

    val filterOct1st2011 = "2011-10-01"
    val imaDinoExclValSet2 = List(KpiConstants.ardtsVal)
    val ardtsNonSupDf = visitNonSuppOptDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.ardtsVal))
                                               &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                               &&($"${KpiConstants.serviceDateColName}".<(filterOct1st2011)))
                                         .select(s"${KpiConstants.memberidColName}").distinct()

    val ardtsOtherMemDf = eligMemForOptDf.except(ardtsNonSupDf).rdd
      .map(r=> r.getString(0))
      .collect()

    val visitArdtsOtherDf = visitForOptDf.filter($"${KpiConstants.memberidColName}".isin(ardvOtherMemDf:_*))

    val ardtsOtherDf = visitArdtsOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.ardtsVal))
                                              &&($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                              &&($"${KpiConstants.serviceDateColName}".<(filterOct1st2011)))
                                        .select(s"${KpiConstants.memberidColName}").distinct()

    val ardtsDf = ardtsNonSupDf.union(ardtsOtherDf).cache()
    ardtsDf.count()
   /* ardtsDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/ardtsDf/")*/


    //</editor-fold>

    //<editor-fold desc="Encephalopathy Due To Vaccination Value Set">

    val inmemForEnc = denominatorPopDf.except(imaTdapTmpDf)

    val visitForEncDf = visitJoinedDf.as("df1").join(inmemForEnc.as("df2"), KpiConstants.memberidColName)
      .select("df1.*").cache()

    val visitForNonSuppEncDf = visitForEncDf.filter($"${KpiConstants.supplflagColName}".===("N"))


    val imaDinoExclValSet3a = List(KpiConstants.encephalopathyVal)
    val edvvcaNonSuppDf = visitForNonSuppEncDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.encephalopathyVal))
                                                   && (array_contains($"${KpiConstants.valuesetColName}", KpiConstants.vaccineAdverseVal))
                                                   && ($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                                   && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months156))))
                                              .select(KpiConstants.memberidColName).distinct()

    val edvvcaOtherMemDf = inmemForEnc.except(edvvcaNonSuppDf)
    val visitedvVcaOtherDf = visitForEncDf.as("df1").join(edvvcaOtherMemDf.as("df2"), KpiConstants.memberidColName)
      .select("df1.*")

    val edvvcaOtherDf = visitedvVcaOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.encephalopathyVal))
                                               && (array_contains($"${KpiConstants.valuesetColName}", KpiConstants.vaccineAdverseVal))
                                               && ($"${KpiConstants.serviceDateColName}".>=($"${KpiConstants.dobColName}"))
                                               && ($"${KpiConstants.serviceDateColName}".<=(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dobColName}",KpiConstants.months156))))
                                          .select(KpiConstants.memberidColName).distinct()

    val edvvcaDf = edvvcaNonSuppDf.union(edvvcaOtherDf).cache()
    edvvcaDf.count()

   /* edvvcaDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/edvvcaDf/")*/

    //</editor-fold>

    val optExclDf = ardvBef13Df.union(ardtsDf).union(edvvcaDf).dropDuplicates().cache()
    optExclDf.count()
    /*optExclDf.select(KpiConstants.memberidColName).coalesce(3)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/optExclDf/")*/

    //</editor-fold>


    val imaMenNumDf = imaMenTmpDf.except(optExclDf).cache()
    imaMenNumDf.count()
    /*imaMenNumDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaMenNumDf/")*/

    val imaHpvNumDf = imaHpvTmpDf.except(optExclDf).cache()
    imaHpvNumDf.count()
   /* imaHpvNumDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaHpvNumDf/")*/

    val imaTdapNumDf = imaTdapTmpDf.except(optExclDf).cache()
    imaTdapNumDf.count()
   /* imaTdapNumDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaTdapNumDf/")*/


    val imaCmb1Df = imaMenNumDf.intersect(imaTdapNumDf).cache()
    imaCmb1Df.count()
    val imaCmb2Df = imaMenNumDf.intersect(imaTdapNumDf).intersect(imaHpvNumDf).cache()
    imaCmb2Df.count()

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutDf, KpiConstants.eligibleDfName -> denominatorPopDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> optExclDf,
      KpiConstants.numeratorDfName -> imaMenNumDf , KpiConstants.numerator2DfName -> imaTdapNumDf,
      KpiConstants.numerator3DfName -> imaHpvNumDf, KpiConstants.numerator4DfName -> imaCmb1Df,
      KpiConstants.numerator5DfName -> imaCmb2Df)


    val outDf = UtilFunctions.ncqaOutputDfCreation1(spark,outMap)
    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/outDf/")


    spark.sparkContext.stop()
  }

}
