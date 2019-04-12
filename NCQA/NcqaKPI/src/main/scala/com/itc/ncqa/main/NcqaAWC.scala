package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable

object NcqaAWC {

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
    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
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

    //<editor-fold desc="Age Filter">

    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageClDate = year+ "-12-31"
    val ageFilterDf = membershipDf.filter((UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",144).<=(ageClDate))
                                        &&(UtilFunctions.add_ncqa_months(spark,$"${KpiConstants.dateofbirthColName}",264).>(ageClDate)))


   /* println("--------------Age Filter -------------------")
    ageFilterDf.show()*/

    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">

    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
      KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
      KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.dateofbirthColName,KpiConstants.primaryPlanFlagColName)

    val benNonMedRemDf = inputForContEnrolldf.filter($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal))
    // val argMapForContEnrollFunction = mutable.Map(KpiConstants.ageStartKeyName -> "12", )
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName,UtilFunctions.add_ncqa_months(spark,lit(ageStartDate), 0))
      .withColumn(KpiConstants.contenrollUppCoName,UtilFunctions.add_ncqa_months(spark,lit(ageEndDate), 0))
      .withColumn(KpiConstants.anchorDateColName, UtilFunctions.add_ncqa_months(spark,lit(ageEndDate), 0))
    /* contEnrollInDf.printSchema()
     contEnrollInDf.show()*/

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

    /*temp3 */
    /* printf("contEnrollStep3Df")
     contEnrollStep3Df.show()*/
    /*temp3 */

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

    /*  printf("contEnrollmemDf")
      contEnrollmemDf.show()*/

    val contEnrollDf = contEnrollStep1Df.as("df1").join(contEnrollmemDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .filter($"df1.${KpiConstants.contEdFlagColName}".===(1))
      .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}",s"df1.${KpiConstants.primaryPlanFlagColName}").cache()
    /*temp3 */


    //</editor-fold>

    //<editor-fold desc="Dual eligibility,Dual enrollment, AWC enrollment filter">

    val baseOutDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList, measureId)

    //baseOutDf.filter($"${KpiConstants.memberidColName}".===("95033")).show()

    val awcContEnrollDf = baseOutDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*)).dropDuplicates().cache()

    // awcContEnrollDf.filter($"${KpiConstants.memberidColName}".===("95033")).show()
    /*awcContEnrollDf.count()
    awcContEnrollDf.filter($"${KpiConstants.memberidColName}".===("106041")).show()
    awcContEnrollDf.select(KpiConstants.memberidColName).distinct().coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/awc/imaContEnrollDf/")*/

    //</editor-fold>

    //<editor-fold desc="Initial Join with Ref_Hedis">

    val argmapforRefHedis = mutable.Map(KpiConstants.eligibleDfName -> visitsDf , KpiConstants.refHedisTblName -> refHedisDf)
    val allValueList = List(KpiConstants.independentLabVal, KpiConstants.hospiceVal,KpiConstants.wellCareVal)


    val medList = KpiConstants.emptyList
    val visitRefHedisDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforRefHedis,allValueList,medList)
    println("visitRefHedisDf")

    //visitRefHedisDf.filter($"${KpiConstants.memberidColName}".===("102916")).show()

    //</editor-fold>

    //<editor-fold desc="Removal of Independent Lab Visits">

    val groupList = visitsDf.schema.fieldNames.toList.dropWhile(p=> p.equalsIgnoreCase(KpiConstants.memberidColName))
    val visitgroupedDf = visitRefHedisDf.groupBy(KpiConstants.memberidColName, groupList:_*).agg(collect_list(KpiConstants.valuesetColName).alias(KpiConstants.valuesetColName))
      .select(KpiConstants.memberidColName, KpiConstants.serviceDateColName, KpiConstants.admitDateColName, KpiConstants.dischargeDateColName,
        KpiConstants.dobColName,KpiConstants.supplflagColName, KpiConstants.ispcpColName,KpiConstants.isobgynColName, KpiConstants.valuesetColName)

    //visitgroupedDf.filter($"${KpiConstants.memberidColName}" === ("102916")).show()

    val indLabVisRemDf = visitgroupedDf.filter(!array_contains($"${KpiConstants.valuesetColName}",KpiConstants.independentLabVal)).repartition(2).cache()
    println("indLabVisRemDf")
    //indLabVisRemDf.filter($"${KpiConstants.memberidColName}" === ("102916")).show()
    //indLabVisRemDf.filter($"${KpiConstants.memberidColName}" === ("95167")).show()

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
      .csv("/home/hbase/ncqa/awc/hospiceInCurrYearMemDf/")

    //</editor-fold>


    val totalPopmemidDf = awcContEnrollDf.select(KpiConstants.memberidColName).except(hospiceInCurrYearMemDf).distinct()
      .rdd
      .map(r=> r.getString(0))
      .collect()


    val totalPopOutDf = awcContEnrollDf.filter($"${KpiConstants.memberidColName}".isin(totalPopmemidDf:_*))
      .select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.payerColName).dropDuplicates()


    val toutStrDf =totalPopOutDf.select($"${KpiConstants.memberidColName}",$"${KpiConstants.memberidColName}".as(KpiConstants.ncqaOutmemberIdCol),$"${KpiConstants.payerColName}".as(KpiConstants.ncqaOutPayerCol))
      .withColumn(KpiConstants.ncqaOutMeasureCol,lit(measureId))


    toutStrDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("D:\\Scala\\AWC\\tout")

    val eligiblePopDf = totalPopOutDf.select(KpiConstants.memberidColName).distinct().repartition(2).cache()
    eligiblePopDf.count()

    eligiblePopDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("D:\\Scala\\AWC\\epop")

    //</editor-fold>

    //<editor-fold desc="Denominator calculation">

    val denominatorDf = eligiblePopDf.distinct()

    denominatorDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("D:\\Scala\\AWC\\denominator")

    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> denominatorDf , KpiConstants.visitTblName -> indLabVisRemDf)
    val visitJoinedOutDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin)
    visitJoinedOutDf.count()
    //visitJoinedOutDf.filter($"${KpiConstants.memberidColName}".===("102916")).show()

    visitJoinedOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("D:\\Scala\\AWC\\visitJoinedDf")


    awcContEnrollDf.unpersist()
    visitgroupedDf.unpersist()
    eligiblePopDf.unpersist()
    denominatorDf.unpersist()

    //println("------------------------visitJoinedOutDf---------------------------")
    //visitJoinedOutDf.show()

    //</editor-fold>

    //<editor-fold desc="AWC Numerator Calculation">

    val visitJoinedDf = spark.read.parquet("D:\\Scala\\AWC\\visitJoinedDf").repartition(2).cache()
    visitJoinedDf.count()

     /*println("------------------visitJoinedDf---------------")
     visitJoinedDf.show()*/

    val toutDf = spark.read.parquet("D:\\Scala\\AWC\\tout")
    val denominatorPopDf = toutDf.select(KpiConstants.memberidColName).distinct().cache()
    /* println("-------------------------denominatorPopDf----------------------")
     denominatorPopDf.show()*/


    //<editor-fold desc="AWC Non Supplement Numerator Calculation">

    val visitNonSuppDf = visitJoinedDf.filter($"${KpiConstants.supplflagColName}".===("N")).cache()
    visitNonSuppDf.count()

    /*println("-------------------------visitNonSuppDf----------------------")
    visitNonSuppDf.show()*/

    val awcMenNonSuppDf = visitNonSuppDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.wellCareVal))
      &&(($"${KpiConstants.ispcpColName}".===("Y"))||($"${KpiConstants.isobgynColName}".===("Y")))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(s"${KpiConstants.memberidColName}")

    /*  println("--------------------awcMenNonSuppDf--------------------")
      awcMenNonSuppDf.show()*/

    //</editor-fold>

    //<editor-fold desc="AWC Other Numerator Calculation">

    val visitForMenOtherDf = visitJoinedDf.except(visitJoinedDf.filter($"${KpiConstants.memberidColName}".isin(
      awcMenNonSuppDf.rdd.map(r=>r.getString(0)).collect():_*
    )))

    val awcNumotherDf = visitForMenOtherDf.filter((array_contains($"${KpiConstants.valuesetColName}",KpiConstants.wellCareVal))
      &&(($"${KpiConstants.ispcpColName}".===("Y"))||($"${KpiConstants.isobgynColName}".===("Y")))
      &&($"${KpiConstants.serviceDateColName}".>=(yearStartDate) && $"${KpiConstants.serviceDateColName}".<=(yearEndDate)))
      .select(s"${KpiConstants.memberidColName}")
    //</editor-fold>

    val awcNumDf = awcMenNonSuppDf.union(awcNumotherDf).dropDuplicates()

  /*  println("-----------numerator---------------------")
    awcNumDf.show()


    awcNumDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("D:\\Scala\\AWC\\awcNumDf")*/

    //</editor-fold>

    //<editor-fold desc="AWC NCQA Output Creation">

    val outMap = mutable.Map(KpiConstants.totalPopDfName -> toutStrDf, KpiConstants.eligibleDfName -> denominatorPopDf,
      KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> spark.emptyDataFrame,
      KpiConstants.numeratorDfName -> awcNumDf )


    val outDf = UtilFunctions.ncqaOutputDfCreation(spark,outMap,measureId)
    outDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("D:\\Scala\\AWC\\outDf")
    //</editor-fold>

    spark.sparkContext.stop()
  }
}
