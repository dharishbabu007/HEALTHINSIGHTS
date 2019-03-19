package com.itc.ncqa.main

import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.SparkObject.spark
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.mutable


object NcqaLSC {

  def main(args: Array[String]): Unit = {

/*
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

    //</editor-fold>

    //<editor-fold desc="Loading Required Tables to memory">

    import spark.implicits._

    val aLiat = List("col1")
    //val generalmembershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.generalmembershipTblName,aLiat)

    val membershipDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.membershipTblName,aLiat)
                                        .filter(($"${KpiConstants.considerationsColName}".===(KpiConstants.yesVal))
                                             && ($"${KpiConstants.memStartDateColName}".isNotNull)
                                             && ($"${KpiConstants.memEndDateColName}".isNotNull))
                                        .cache()

    val visitDf = DataLoadFunctions.dataLoadFromHiveStageTable(spark,KpiConstants.dbName,KpiConstants.visitTblName,aLiat)
                                   .filter(($"${KpiConstants.serviceDateColName}".isNotNull)
                                        &&(($"${KpiConstants.admitDateColName}".isNotNull && $"${KpiConstants.dischargeDateColName}".isNotNull)
                                        || ($"${KpiConstants.admitDateColName}".isNull && $"${KpiConstants.dischargeDateColName}".isNull)))
                                   .cache()

    val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
                                      .filter($"${KpiConstants.measureIdColName}".===(KpiConstants.lscMeasureId))
                                      .cache()

    val ref_medvaluesetDf = DataLoadFunctions.referDataLoadFromTragetModel(spark,KpiConstants.dbName,KpiConstants.refmedValueSetTblName)
                                             .filter($"${KpiConstants.measure_idColName}".===(KpiConstants.lscMeasureId))
                                             .cache()

    //</editor-fold>

    //<editor-fold desc="Eligible Population Calculation">

    //<editor-fold desc="Initial join function">

    val argmapForInitJoin = mutable.Map( KpiConstants.membershipTblName -> membershipDf, KpiConstants.visitTblName -> visitDf)
    val initialJoinedDf = UtilFunctions.initialJoinFunction(spark,argmapForInitJoin).cache()
    initialJoinedDf.count()
    // println("initialJoinedDf.count():"+initialJoinedDf.count())
    //</editor-fold>

    //<editor-fold desc="Hospice Removal">

    val argmapforHospice = mutable.Map(KpiConstants.eligibleDfName -> initialJoinedDf, KpiConstants.refHedisTblName -> refHedisDf
      ,KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val hospiceValList = List(KpiConstants.hospiceVal)
    val hospiceCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.hcpsCodeVal, KpiConstants.ubrevCodeVal, KpiConstants.ubtobCodeVal)
    val hospiceClaimsDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapforHospice,hospiceValList,hospiceCodeSystem)
    val hospiceincurryearDf = UtilFunctions.measurementYearFilter(hospiceClaimsDf,KpiConstants.serviceDateColName,year,KpiConstants.measurement0Val, KpiConstants.measurement0Val)
      .select(KpiConstants.memberidColName)

    var hospiceRemovedClaimsDf = spark.emptyDataFrame
    if (hospiceincurryearDf.count()> 0){
      hospiceRemovedClaimsDf = initialJoinedDf.as("df1").join(hospiceincurryearDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
        .filter($"df2.${KpiConstants.memberidColName}".isNull)
        .select("df1.*").cache()
    }
    else {
      hospiceRemovedClaimsDf = initialJoinedDf.cache()
    }
    hospiceRemovedClaimsDf.count()
    initialJoinedDf.unpersist()
    //</editor-fold>

    //<editor-fold desc="Age Filter">

    val age_filter_upperDate = year + "-12-31"
    val age_filter_lowerDate = year + "-01-01"
    val ageFilterDf = hospiceRemovedClaimsDf.filter((add_months($"${KpiConstants.dobColName}", KpiConstants.months24).>=(age_filter_lowerDate)) && (add_months($"${KpiConstants.dobColName}", KpiConstants.months24).<=(age_filter_upperDate)))
    //</editor-fold>

    //<editor-fold desc="Continuous Enrollment, Allowable Gap and Benefit">


    val contEnrollStartDate = year + "-01-01"
    val contEnrollEndDate = year + "-12-31"
    val inputForContEnrolldf = ageFilterDf.select(KpiConstants.memberidColName, KpiConstants.benefitMedicalColname,
                                                  KpiConstants.memStartDateColName,KpiConstants.memEndDateColName,
                                                  KpiConstants.lobColName, KpiConstants.dobColName)

    val contEnrollInDf = inputForContEnrolldf.withColumn(KpiConstants.contenrollLowCoName, add_months($"${KpiConstants.dobColName}", KpiConstants.months12))
                                             .withColumn(KpiConstants.contenrollUppCoName, add_months($"${KpiConstants.dobColName}", KpiConstants.months24))
                                             .withColumn(KpiConstants.anchorDateColName, add_months($"${KpiConstants.dobColName}", KpiConstants.months24))


    /*step1 (find out the members whoose either mem_start_date or mem_end_date should be in continuous enrollment period)*/
    val contEnrollStep1Df = contEnrollInDf.filter((($"${KpiConstants.memStartDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.contenrollUppCoName}")))
      || (($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.contenrollLowCoName}")) && ($"${KpiConstants.memEndDateColName}".<=($"${KpiConstants.contenrollUppCoName}"))))



    val contEnrollStep2Df = contEnrollStep1Df.withColumn(KpiConstants.benefitMedicalColname, when($"${KpiConstants.benefitMedicalColname}".===(KpiConstants.yesVal), 1).otherwise(0))




    /*Step3(select the members who satisfy both (min_start_date- ces and cee- max_end_date <= allowable gap) conditions)*/
    val listDf = contEnrollStep2Df.groupBy($"${KpiConstants.memberidColName}")
      .agg(max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName))
      .filter(((date_add($"max_mem_end_date",KpiConstants.days45+1).>=($"${KpiConstants.contenrollUppCoName}"))
        && (date_sub($"min_mem_start_date",KpiConstants.days45 +1).<=($"${KpiConstants.contenrollLowCoName}"))))
      .select($"${KpiConstants.memberidColName}")

    val contEnrollStep3Df = contEnrollStep2Df.as("df1").join(listDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
      .select("df1.*")


    // contEnrollStep3Df.printSchema()
    /*window function creation based on partioned by member_sk and order by mem_start_date*/
    val contWindowVal = Window.partitionBy(s"${KpiConstants.memberidColName}").orderBy(s"${KpiConstants.memStartDateColName}")


    /* added 3 columns (date_diff(datediff b/w next start_date and current end_date for each memeber),
     anchorflag(if member is continuously enrolled on anchor date 1, otherwise 0)
     count(if date_diff>1 1, otherwise 0) over window*/
    val contEnrollStep4Df = contEnrollStep3Df.withColumn(KpiConstants.datediffColName, datediff(lead($"${KpiConstants.memStartDateColName}",1).over(contWindowVal), $"${KpiConstants.memEndDateColName}"))
      .withColumn(KpiConstants.anchorflagColName,when( ($"${KpiConstants.memStartDateColName}".<=($"${KpiConstants.anchorDateColName}"))
        && ($"${KpiConstants.memEndDateColName}".>=($"${KpiConstants.anchorDateColName}"))
        && ($"${KpiConstants.lobColName}".===(lob_name)), lit(1)).otherwise(lit(0)))
      .withColumn(KpiConstants.countColName, when($"${KpiConstants.datediffColName}".>(1),lit(1)).otherwise(lit(0)) )



    val contEnrollStep5Df = contEnrollStep4Df.groupBy(KpiConstants.memberidColName)
      .agg(min($"${KpiConstants.memStartDateColName}").alias(KpiConstants.minMemStDateColName),
        max($"${KpiConstants.memEndDateColName}").alias(KpiConstants.maxMemEndDateColName),
        max($"${KpiConstants.datediffColName}").alias(KpiConstants.maxDateDiffColName),
        sum($"${KpiConstants.countColName}").alias(KpiConstants.countColName),
        sum($"${KpiConstants.anchorflagColName}").alias(KpiConstants.anchorflagColName),
        first($"${KpiConstants.contenrollLowCoName}").alias(KpiConstants.contenrollLowCoName),
        first($"${KpiConstants.contenrollUppCoName}").alias(KpiConstants.contenrollUppCoName),
        sum($"${KpiConstants.benefitMedicalColname}").alias(KpiConstants.sumBenefitColName),
        count($"${KpiConstants.benefitMedicalColname}").alias(KpiConstants.countBenefitColName))



    val contEnrollmemDf = contEnrollStep5Df.filter((($"${KpiConstants.maxDateDiffColName}" - 1).<=(KpiConstants.days45) || ($"${KpiConstants.maxDateDiffColName}" - 1).isNull)
      && ($"${KpiConstants.anchorflagColName}".>(0))
      && ((($"${KpiConstants.countColName}")
      + (when(date_sub($"min_mem_start_date", 1).>($"${KpiConstants.contenrollLowCoName}"),lit(1)).otherwise(lit(0)))
      + (when(date_add($"max_mem_end_date", 1).<($"${KpiConstants.contenrollUppCoName}"),lit(1)).otherwise(lit(0)))).<=(1) )
      && ($"${KpiConstants.sumBenefitColName}".===($"${KpiConstants.countBenefitColName}")))
      .select(KpiConstants.memberidColName)


    //val contEnrollmemDf = UtilFunctions.contEnrollAndAllowableGapFilter(spark,inputForContEnrolldf,KpiConstants.commondateformatName,argMap)

    val contEnrollDf = ageFilterDf.as("df1").join(contEnrollmemDf.as("df2"),$"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.innerJoinType)
                                                   .select("df1.*").cache()
    //val table1 = contEnrollDf.createTempView("contEnrollTable")
    println(contEnrollDf.count())
    ageFilterDf.unpersist()
    //contEnrollDf.printSchema()
    //println("contEnroll count:"+contEnrollDf.count())

    //</editor-fold>

    val totalPopDf = contEnrollDf

    val eligibleDf = totalPopDf.select(KpiConstants.memberidColName)
    eligibleDf.show()
    //</editor-fold>

    //<editor-fold desc="Dinominator Calculation">

    val dinominatorDf = eligibleDf

    //</editor-fold>

    //<editor-fold desc="Numerator Calculation">

    //<editor-fold desc="Numerator Calculation for Non supplement Data">

    val claimStatusList = List(KpiConstants.paidVal, KpiConstants.suspendedVal, KpiConstants.pendingVal, KpiConstants.deniedVal)

    val eligNumsuppNDf = totalPopDf.filter(($"${KpiConstants.supplflagColName}".===("N"))
                                        && ($"${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))

    val argmapNumSuppNdata = mutable.Map(KpiConstants.eligibleDfName -> eligNumsuppNDf, KpiConstants.refHedisTblName -> refHedisDf,
                                         KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    /*Numerator Calculation*/
    val leadTestValList = List(KpiConstants.leadTestVal)
    val leadTestCodeSystem = List(KpiConstants.cptCodeVal, KpiConstants.loincCodeVal,KpiConstants.hcpsCodeVal)
    val numeratorNonSuppDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapNumSuppNdata,leadTestValList,leadTestCodeSystem)
                                               .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months24)))
                                               .select(KpiConstants.memberidColName)


    //</editor-fold>

    //<editor-fold desc="Numerator Calculation for Other Data">

    val eligNumsuppDf = totalPopDf.as("df1").join(numeratorNonSuppDf.as("df2"), $"df1.${KpiConstants.memberidColName}" === $"df2.${KpiConstants.memberidColName}", KpiConstants.leftOuterJoinType)
                                                   .filter(($"df2.${KpiConstants.memberidColName}".isNull) && ($"df1.${KpiConstants.claimstatusColName}".isin(claimStatusList:_*)))

    val argmapNumSuppdata = mutable.Map(KpiConstants.eligibleDfName -> eligNumsuppDf, KpiConstants.refHedisTblName -> refHedisDf,
                                        KpiConstants.refmedValueSetTblName -> ref_medvaluesetDf)

    val numeratorSuppDf = UtilFunctions.joinWithRefHedisFunction(spark,argmapNumSuppNdata,leadTestValList,leadTestCodeSystem)
                                       .filter($"${KpiConstants.serviceDateColName}".<=(add_months($"${KpiConstants.dobColName}",KpiConstants.months24)))
                                       .select(KpiConstants.memberidColName)
    //</editor-fold>

    val numeratorDf = numeratorNonSuppDf.union(numeratorSuppDf)
    numeratorDf.show()
    //</editor-fold>



    //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

    /*Common output format (data to fact_hedis_gaps_in_care)*/
    val numeratorValueSet = leadTestValList
    val dinominatorExclValueSet = KpiConstants.emptyList
    val numeratorExclValueSet = KpiConstants.emptyList
    val listForOutput = List(numeratorValueSet,dinominatorExclValueSet,numeratorExclValueSet)

    /*add sourcename and measure id into a list*/
    val sourceAndMsrIdList = List(data_source,measureId)

    val numExclDf = spark.emptyDataFrame
    val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, hospiceDf, numeratorDf, numExclDf, listForOutput, sourceAndMsrIdList)
    outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
    //</editor-fold>
*/
    spark.sparkContext.stop()
  }

}
