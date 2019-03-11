package com.itc.ncqa.main

import java.sql.Date

import com.itc.ncqa.Constants
import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.sql.SparkSession
//import com.itc.ncqa.Functions.SparkObject._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.mutable

case class Member(member_id:String, service_date:Date)
//case class GroupMember(member_id:String, dateList:List[Long])

object NcqaIMA {


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
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._


    val aLiat = List("col1")

    val lobList = List(KpiConstants.commercialLobName, KpiConstants.medicaidLobName, KpiConstants.marketplaceLobName, KpiConstants.mmdLobName)
    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))
                                        .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                        .withColumn(KpiConstants.memStartDateColName, to_date($"${KpiConstants.memStartDateColName}", KpiConstants.dateFormatString))
                                        .withColumn(KpiConstants.memEndDateColName, to_date($"${KpiConstants.memEndDateColName}", KpiConstants.dateFormatString))
                                        .withColumn(KpiConstants.dateofbirthColName, to_date($"${KpiConstants.dateofbirthColName}", KpiConstants.dateFormatString))


 /*   membershipDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/membership/")*/
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
      .withColumn(KpiConstants.revenuecodeColName, when((length($"${KpiConstants.revenuecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.revenuecodeColName}"))).otherwise($"${KpiConstants.revenuecodeColName}"))
      .withColumn(KpiConstants.billtypecodeColName, when((length($"${KpiConstants.billtypecodeColName}").as[Int].===(3)),concat(lit("0" ),lit($"${KpiConstants.billtypecodeColName}"))).otherwise($"${KpiConstants.billtypecodeColName}"))
      .withColumn(KpiConstants.proccode2ColName, when(($"${KpiConstants.proccode2mod1ColName}".isin(KpiConstants.avoidCodeList:_*)) || ($"${KpiConstants.proccode2mod2ColName}".isin(KpiConstants.avoidCodeList:_*)),lit("NA")).otherwise($"${KpiConstants.proccode2ColName}"))



    val medmonmemDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.medmonmemTblName,aLiat)
                                       .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")

    val productPlanDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.productPlanTblName,aLiat)
                                         .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")



    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter(($"${KpiConstants.measureIdColName}".===(KpiConstants.imaMeasureId)) || ($"${KpiConstants.measureIdColName}".===(KpiConstants.ggMeasureId)))
                                      .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                      .cache()
    refHedisDf.count()

    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.imaMeasureId))
                                             .drop("latest_flag", "curr_flag", "active_flag", "ingestion_date", "rec_update_date" , "source_name" , "rec_create_date", "user_name")
                                             .cache()

    val argMapForValidVisits = mutable.Map(KpiConstants.eligibleDfName -> visitsDf, KpiConstants.refHedisTblName -> refHedisDf)
    val invalidVistValList = List(KpiConstants.independentLabVal)
    val invalidVisitsDf = UtilFunctions.joinWithRefHedisFunction(spark,argMapForValidVisits,invalidVistValList,KpiConstants.codeSystemList)
    val validVisitsDf = visitsDf.except(invalidVisitsDf)
    //val validVisitsDf = invalidVisitsRemovedDf.withColumn(KpiConstants.proccode2ColName, when(($"${KpiConstants.proccode2mod1ColName}".isin(KpiConstants.avoidCodeList:_*)) || ($"${KpiConstants.proccode2mod2ColName}".isin(KpiConstants.avoidCodeList:_*)),lit("ignore")).otherwise($"${KpiConstants.proccode2ColName}"))

    //</editor-fold

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Age filter">

    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"

    val ageFilterDf = membershipDf.filter((add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months156).>=(ageStartDate)) && (add_months($"${KpiConstants.dateofbirthColName}",KpiConstants.months156).<=(ageEndDate)))

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
   // val argMapForContEnrollFunction = mutable.Map(KpiConstants.ageStartKeyName -> "12", )
    val contEnrollInDf = benNonMedRemDf.withColumn(KpiConstants.contenrollLowCoName, add_months($"${KpiConstants.dateofbirthColName}", KpiConstants.months144))
                                             .withColumn(KpiConstants.contenrollUppCoName, add_months($"${KpiConstants.dateofbirthColName}", KpiConstants.months156))
                                             .withColumn(KpiConstants.anchorDateColName, add_months($"${KpiConstants.dateofbirthColName}", KpiConstants.months156))



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
                                        .select(s"df1.${KpiConstants.memberidColName}", s"df1.${KpiConstants.lobColName}", s"df1.${KpiConstants.lobProductColName}",s"df1.${KpiConstants.payerColName}",s"df1.${KpiConstants.primaryPlanFlagColName}")


  /*  contEnrollDf.select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName, KpiConstants.primaryPlanFlagColName).dropDuplicates()
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/contEnrolldetail/")
*/

    //</editor-fold>

    val baseOutDf = UtilFunctions.baseOutDataframeCreation(spark, contEnrollDf, lobList)

   /* baseOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/baseOut/")*/

    val imaContEnrollDf = baseOutDf.filter($"${KpiConstants.lobColName}".isin(lobList:_*))

   /* imaContEnrollDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/imaContEnrollDf/")*/

    //<editor-fold desc="Hospice Removal">

    val argmapforHospice = mutable.Map(KpiConstants.eligibleDfName -> validVisitsDf , KpiConstants.refHedisTblName -> refHedisDf)

    val hospiceValList = List(KpiConstants.hospiceVal)
    val hospiceCodeSystem = KpiConstants.codeSystemList
    val hospiceClaimsDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforHospice,hospiceValList,hospiceCodeSystem)
    val hospiceincurryearDf = UtilFunctions.measurementYearFilter(hospiceClaimsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                           .select(KpiConstants.memberidColName).distinct()


   /* hospiceincurryearDf.select(KpiConstants.memberidColName).coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/hospice/")
*/

    val contEnrollMemDf = imaContEnrollDf.select(KpiConstants.memberidColName).distinct()

    val hosremMemidDf = contEnrollMemDf.except(hospiceincurryearDf)

    val hospiceRemovedMemsDf  = imaContEnrollDf.as("df1").join(hosremMemidDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                             .select("df1.*").cache()
    hospiceRemovedMemsDf.count()
    //</editor-fold>


    //val eligibleMemDf = hospiceRemovedMemsDf.select(KpiConstants.memberidColName).distinct().intersect(validVisitsDf.select(KpiConstants.memberidColName)).dropDuplicates()
    val totalPopOutDf = hospiceRemovedMemsDf.select(KpiConstants.memberidColName,KpiConstants.lobColName, KpiConstants.lobProductColName, KpiConstants.payerColName).dropDuplicates().cache()




    totalPopOutDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/tout/")
    /*eligble population for IMA measure*/
    val eligiblePopDf = totalPopOutDf.select(KpiConstants.memberidColName).distinct().cache()


    //</editor-fold>

/*

    //<editor-fold desc="Dinominator calculation">

    val denominatorPopDf = eligiblePopDf

    //dinominatorDf.show()
    //</editor-fold>

    //<editor-fold desc="Initial join function">

    val argmapForVisittJoin = mutable.Map(KpiConstants.membershipTblName -> denominatorPopDf , KpiConstants.visitTblName -> visitsDf)
    val visitJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForVisittJoin).cache()
    visitJoinedDf.count()
    //</editor-fold>


    val dfMapForCalculation = mutable.Map(KpiConstants.eligibleDfName -> visitJoinedDf,KpiConstants.refHedisTblName -> refHedisDf
                                          ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)





*/






    /*

    //<editor-fold desc="Optional Exclusion Calculation">


    /*Dinominator Exclusion1(Anaphylactic Reaction Due To Vaccination)*/


    //<editor-fold desc="Find memebers who has any of the 3 vaccines">

    val valList = List(KpiConstants.meningococcalVal,KpiConstants.hpvVal,KpiConstants.tdapVaccineVal)
    val codeSystem = List(KpiConstants.cptCodeVal, KpiConstants.cvxCodeVal)
    val joinedForDinoExcl1Df = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, valList, codeSystem)
    val anyVaccineDf = if (joinedForDinoExcl1Df.count()>0){UtilFunctions.measurementYearFilter(joinedForDinoExcl1Df,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                   .select(KpiConstants.memberidColName)} else {spark.createDataFrame(spark.sparkContext.emptyRDD[Row], KpiConstants.memberIdSchema)}
    //</editor-fold>

    //<editor-fold desc="Tdap vaccines">

    val tdapvalList = List(KpiConstants.tdapVaccineVal)
    val joinedForTdap = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, tdapvalList, codeSystem)
    val tdapVaccineDf = if (joinedForTdap.count()>0 ){UtilFunctions.measurementYearFilter(joinedForTdap,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
                                            .select(KpiConstants.memberidColName)} else {spark.createDataFrame(spark.sparkContext.emptyRDD[Row], KpiConstants.memberIdSchema)}
    //</editor-fold>


    /*Find out the members who has not the vaccines based on the measure id*/
    val membersDf = measureId match {


      case KpiConstants.imatdMeasureId => eligiblePopDf.select(KpiConstants.memberidColName).except(tdapVaccineDf)

      case KpiConstants.imamenMeasureId => eligiblePopDf.select(KpiConstants.memberidColName).except(anyVaccineDf)

      case KpiConstants.imahpvMeasureId => eligiblePopDf.select(KpiConstants.memberidColName).except(anyVaccineDf)

      case KpiConstants.imacmb1MeasureId => eligiblePopDf.select(KpiConstants.memberidColName).except(anyVaccineDf)

      case KpiConstants.imacmb2MeasureId => eligiblePopDf.select(KpiConstants.memberidColName).except(anyVaccineDf)
    }


    //<editor-fold desc="ARDV on or before 13th birth day">

    val imaDinoExclValSet1 = List(KpiConstants.ardvVal)
    val ardvBef13yeardf = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, imaDinoExclValSet1, codeSystem)
                                       .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                       .select(s"${KpiConstants.memberidColName}")
    //</editor-fold>

    //<editor-fold desc="Anaphylactic Reaction Due To Serum Value Set">

    val filterOct1st2011 = "2011-10-01"
    val imaDinoExclValSet2 = List(KpiConstants.ardtsVal)
    val ardtsrBefOct1st2011Df = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, imaDinoExclValSet2, codeSystem)
                                             .filter($"${KpiConstants.serviceDateColName}".<=(filterOct1st2011))
                                             .select(s"${KpiConstants.memberidColName}")
    //</editor-fold>

    //<editor-fold desc="Encephalopathy Due To Vaccination Value Set">

    val imaDinoExclValSet3a = List(KpiConstants.encephalopathyVal)
    val edvBefore13YearDf = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, imaDinoExclValSet3a, codeSystem)
                                         .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))



    /*Dinominator Exclusion3and (Vaccine Causing Adverse Effect Value Set) */
    val dfMapForvcae = mutable.Map(KpiConstants.eligibleDfName -> visitJoinedDf,KpiConstants.refHedisTblName -> refHedisDf,
                                   KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val imaDinoExclValSet3b = List(KpiConstants.vaccineAdverseVal)
    val vcabefore13YearDf = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForvcae, imaDinoExclValSet3b, codeSystem)
                                         .select(s"${KpiConstants.memberidColName}")

    /* Encephalopathy Due To Vaccination Value Set with Vaccine Causing Adverse Effect Value Set */
    val imaExlc3Df = vcabefore13YearDf
    //</editor-fold>


    /*Optional exclusion based on the Measure id*/
    val optionalDinoxclDf = measureId match {

      case KpiConstants.imamenMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).dropDuplicates()

      case KpiConstants.imatdMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df).dropDuplicates()

      case KpiConstants.imahpvMeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).dropDuplicates()

      case KpiConstants.imacmb1MeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df).dropDuplicates()

      case KpiConstants.imacmb2MeasureId => ardvBef13yeardf.union(ardtsrBefOct1st2011Df).union(imaExlc3Df).dropDuplicates()
    }

    val optionalExclDf = membersDf.intersect(optionalDinoxclDf).cache()


    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    val memberidForNumCal = denominatorPopDf.except(optionalExclDf)
    val inputForNumDf = visitJoinedDf.as("df1").join(memberidForNumCal.as("df2"), KpiConstants.memberidColName)
    inputForNumDf.count()
    visitJoinedDf.unpersist()

    //<editor-fold desc="Numerator Non Supplemental Data">

    val inForNumNonSup = inputForNumDf.filter($"${KpiConstants.supplflagColName}".===(KpiConstants.noVal))
    val argMapForNumNonSupp = mutable.Map(KpiConstants.eligibleDfName -> inForNumNonSup,KpiConstants.refHedisTblName -> refHedisDf
                                         ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val imaHpvForNonSupDf = UtilFunctions.imaNumeratorCalculationFunction(spark,argMapForNumNonSupp)

    //</editor-fold>


    val inForOtherData = inputForNumDf.as("df1").join(imaHpvForNonSupDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
      .filter($"df2.${KpiConstants.memberidColName}".isNull)
      .select("df1.*")

    val argMapForNumOther = mutable.Map(KpiConstants.eligibleDfName -> inForOtherData,KpiConstants.refHedisTblName -> refHedisDf
                                       ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)



    val imaHpvForNonOtherDf = UtilFunctions.imaNumeratorCalculationFunction(spark,argMapForNumOther)


    val numeratorDf = imaHpvForNonSupDf.union(imaHpvForNonOtherDf).cache()
    numeratorDf.count()


    val mapForOutput = mutable.Map(KpiConstants.totalPopDfName -> totalPopOutDf, KpiConstants.eligibleDfName -> eligiblePopDf,
                                   KpiConstants.mandatoryExclDfname -> spark.emptyDataFrame, KpiConstants.optionalExclDfName -> optionalExclDf,
                                   KpiConstants.numeratorDfName -> numeratorDf)

    val outputDf = UtilFunctions.ncqaOutputDfCreation(spark,mapForOutput,measureId)

    outputDf.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv("/home/hbase/ncqa/ima_test_out/out/")

    /*
    //<editor-fold desc="IMAMEN">

    /*Numerator1 Calculation (IMAMEN screening or monitoring test)*/
    val imaMenValueSet = List(KpiConstants.meningococcalVal)
    val imaMenCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imamenNumDf = UtilFunctions.joinWithRefHedisFunction(spark, dfMapForCalculation, imaMenValueSet, imaMenCodeSystem)
                                             .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}", KpiConstants.months132)))
                                                  && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}", KpiConstants.months156))))
                                             .select(s"${KpiConstants.memberidColName}")

    //</editor-fold>

    //<editor-fold desc="IMATDAP">

    /*Numerator2 Calculation (IMATD screening or monitoring test)*/
    val imaTdapValueSet = List(KpiConstants.tdapVaccineVal)
    val imaTdapCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imaTdapNumDf = UtilFunctions.dimMemberFactClaimHedisJoinFunction(spark,dfMapForCalculation, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, KpiConstants.imaMeasureId, imaTdapValueSet, imaTdapCodeSystem)
                                              .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}",KpiConstants.months120))) && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months156)))
                                                       && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList)))
                                              .select(s"${KpiConstants.memberskColName}")

    //</editor-fold>

     //<editor-fold desc="IMAHPV">

    /*Numerator3 Calculation (IMAHPV screening or monitoring test)*/
    val imaHpvValueSet = List(KpiConstants.hpvVal)
    val imaHpvCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.cvxCodeVal)
    val imaHpvAgeFilterDf = UtilFunctions.joinWithRefHedisFunction(spark, argMapForNumNonSupp, imaHpvValueSet, imaHpvCodeSystem)
                                         .filter(($"${KpiConstants.serviceDateColName}".>=(add_months($"${KpiConstants.dobColName}", KpiConstants.months108)))
                                              && ($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}", KpiConstants.months156))))
                                         .select(s"${KpiConstants.memberidColName}", s"${KpiConstants.serviceDateColName}")




    /*HPV First Condition(atleast 2 date of service with 146 days gap)*/
    val imaHpv1Df = imaHpvAgeFilterDf.groupBy(KpiConstants.memberidColName).agg(count(when(datediff(max($"${KpiConstants.serviceDateColName}"), min($"${KpiConstants.serviceDateColName}")).>=(146), 1)).alias(KpiConstants.countColName))
                                     .filter($"${KpiConstants.countColName}".>=(KpiConstants.count1Val))
                                     .select(KpiConstants.memberidColName)



    val imaHpv2InDf = imaHpvAgeFilterDf.as("df1").join(imaHpv1Df.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                                        .filter($"df2.${KpiConstants.memberidColName}".isNull)
                                                        .select("df1.*")

    val inDs = imaHpv2InDf.as[Member]

    val groupedDs = inDs.groupByKey(inDs => (inDs.member_id))
      .mapGroups((k,itr) => (k,itr.map(f=> f.service_date.getTime).toArray.sorted))


    val imaHpv2Df = groupedDs.map(f=> UtilFunctions.getMembers(f._1,f._2))
      .filter(f=> f._2.equals("Y")).select("_1").toDF("member_id")


    val imaHpvDf = imaHpv1Df.union(imaHpv2Df).dropDuplicates()
    //</editor-fold>


    /*Numerator4 Calculation (Combination 1 (Meningococcal, Tdap))*/
    val imaCmb1Df = imamenNumDf.intersect(imaTdapNumDf)

    /*Numerator5 Calculation (Combination 2 (Meningococcal, Tdap, HPV))*/
    val imaCmb2Df = imamenNumDf.intersect(imaTdapNumDf).intersect(imaHpvDf)


    /*numeratorDf and the numerator vcalueset based on the measure id*/
    var imaNumeratorDf = spark.emptyDataFrame
    var numeratorVal = KpiConstants.emptyList
    measureId match {

      case KpiConstants.imamenMeasureId  => imaNumeratorDf = imamenNumDf
                                            numeratorVal = imaMenValueSet

      case KpiConstants.imatdMeasureId   => imaNumeratorDf = imaTdapNumDf
                                            numeratorVal = imaTdapValueSet

      case KpiConstants.imahpvMeasureId  => imaNumeratorDf = imaHpvDf
                                            numeratorVal = imaHpvValueSet

      case KpiConstants.imacmb1MeasureId => imaNumeratorDf = imaCmb1Df
                                            numeratorVal = imaMenValueSet:::imaTdapValueSet

      case KpiConstants.imacmb2MeasureId => imaNumeratorDf = imaCmb2Df
                                            numeratorVal = imaMenValueSet:::imaTdapValueSet:::imaHpvValueSet

    }

    val numeratorDf =  imaNumeratorDf
    //numeratorDf.show()

    */

    //</editor-fold>

*/
    spark.sparkContext.stop()
  }

}
