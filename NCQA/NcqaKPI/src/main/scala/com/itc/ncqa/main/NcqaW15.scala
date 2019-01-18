package com.itc.ncqa.main


import com.itc.ncqa.Constants.KpiConstants
import com.itc.ncqa.Functions.{DataLoadFunctions, UtilFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConversions._

object NcqaW15 {

  def main(args: Array[String]): Unit = {



      //<editor-fold desc="Reading program arguments and spark session Object creation">

      /*Reading the program arguments*/
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

      /*creating spark session object*/
      val conf = new SparkConf().setMaster("local[*]").setAppName("NCQAW15")
      conf.set("hive.exec.dynamic.partition.mode","nonstrict")
      val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

      import spark.implicits._
      //</editor-fold>

      //<editor-fold desc="Loading of Required Tables">

      /*Loading dim_member,fact_claims,fact_membership tables */
      val dimMemberDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimMemberTblName, data_source)
      val factClaimDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factClaimTblName, data_source)
      val factMembershipDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.factMembershipTblName, data_source)
      val dimLocationDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimLocationTblName, data_source)
      val refLobDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refLobTblName)
      val dimFacilityDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimFacilityTblName, data_source).select(KpiConstants.facilitySkColName)
      val dimProviderDf = DataLoadFunctions.dataLoadFromTargetModel(spark, KpiConstants.dbName, KpiConstants.dimProviderTblName, data_source)
      /*loading ref_hedis table*/
      val refHedisDf = DataLoadFunctions.referDataLoadFromTragetModel(spark, KpiConstants.dbName, KpiConstants.refHedisTblName)
      val primaryDiagnosisCodeSystem = KpiConstants.primaryDiagnosisCodeSystem

      //</editor-fold>

      //<editor-fold desc="Initial Join,Allowable Gap filter Calculations, Age filter and Continous enrollment">

      /*Initial join function call for prepare the data fro common filter*/
      val initialJoinedDf = UtilFunctions.joinForCommonFilterFunction(spark, dimMemberDf, factClaimDf, factMembershipDf, dimLocationDf, refLobDf, dimFacilityDf, lob_name, KpiConstants.w15MeasureTitle)

    /*Allowable gap filtering*/
    var lookUpDf = spark.emptyDataFrame
    if ((KpiConstants.commercialLobName.equalsIgnoreCase(lob_name)) ) {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view45Days)
    }
    else {

      lookUpDf = DataLoadFunctions.viewLoadFunction(spark, KpiConstants.view60Days)
    }
    val commonFilterDf = initialJoinedDf.as("df1").join(lookUpDf.as("df2"), initialJoinedDf.col(KpiConstants.memberskColName) === lookUpDf.col(KpiConstants.memberskColName), KpiConstants.leftOuterJoinType).filter(lookUpDf.col("start_date").isNull).select("df1.*")

    /*doing age filter 15 months old during the measurement year */
    val ageEndDate = year + "-12-31"
    val ageStartDate = year + "-01-01"
    val month15ColAddedDf = commonFilterDf.withColumn(KpiConstants.month15ColName, add_months(commonFilterDf.col(KpiConstants.dobColName),15))
    val ageFilterDf = month15ColAddedDf.filter((month15ColAddedDf.col(KpiConstants.month15ColName).>=(ageStartDate)) && (month15ColAddedDf.col(KpiConstants.month15ColName).<=(ageEndDate)))

    /*Continuous Enrollment Checking*/
    val contEnrollDf = ageFilterDf.filter((ageFilterDf.col(KpiConstants.memStartDateColName).<=(date_add(ageFilterDf.col(KpiConstants.dobColName),KpiConstants.days31)))
      && (ageFilterDf.col(KpiConstants.memEndDateColName).>=(ageFilterDf.col(KpiConstants.month15ColName))))

    //</editor-fold>

      //<editor-fold desc="Dinominator calculation">

      val dinominatorDf = contEnrollDf
      val dinominatorForKpiCalDf = dinominatorDf.select(KpiConstants.memberskColName).distinct()
      //dinominatorDf.show()
      //</editor-fold>

      //<editor-fold desc="Dinominator Exclusion Calculation">

      /*find out the hospice members*/
      val hospiceDf = UtilFunctions.hospiceFunction(spark,factClaimDf,refHedisDf).select(KpiConstants.memberskColName).distinct()
      val dinominatorExclDf = hospiceDf
      /*Dinominator After Exclusion*/
      val dinominatorAfterExclusionDf = dinominatorForKpiCalDf.except(hospiceDf)
      //dinominatorAfterExclusionDf.show()

      //</editor-fold>

      //<editor-fold desc="Numerator Calculation">

      /*Numerator Calculation (Well-Care Value Set) as procedure code*/
      val w15NumeratorVal = List(KpiConstants.wellCareVal)
      val w15NumCodeSystem = List(KpiConstants.cptCodeVal,KpiConstants.hcpsCodeVal)
      val joinedForWellCareDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.proceedureCodeColName, KpiConstants.innerJoinType, measureId, w15NumeratorVal, w15NumCodeSystem)
                                             .select(KpiConstants.memberskColName,KpiConstants.startDateColName, KpiConstants.providerSkColName)



      /*Numerator Calculation (Well-Care Value Set) as primary diagnosis*/
      val joinedForWellCareAsDiagDf = UtilFunctions.factClaimRefHedisJoinFunction(spark, factClaimDf, refHedisDf, KpiConstants.primaryDiagnosisColname, KpiConstants.innerJoinType, measureId , w15NumeratorVal, primaryDiagnosisCodeSystem)
                                                   .select(KpiConstants.memberskColName,KpiConstants.startDateColName,  KpiConstants.providerSkColName)

      /*Numerator Calculation (Union of  measurementForWellCareAsPrDf and measurementForWellCareAsDiagDf)*/
      val wellCareUnionDf = joinedForWellCareDf.union(joinedForWellCareAsDiagDf)

      val w15NumeratorGeneral = wellCareUnionDf.as("df1").join(contEnrollDf.as("df2"),$"df1.${KpiConstants.memberskColName}" === $"df2.${KpiConstants.memberskColName}",KpiConstants.innerJoinType)
                                                                    .filter($"df1.${KpiConstants.startDateColName}".<=($"df2.${KpiConstants.month15ColName}"))
                                                                    .select("df1.*")
      /*Start_date_sk for members who have pcp equal to 'Y'*/

      val w15WellCareJoinDf = dimProviderDf.as("df1").join(w15NumeratorGeneral.as("df2"), ($"df1.$KpiConstants.providerSkColName" === $"df2.$KpiConstants.providerSkColName"), joinType = KpiConstants.innerJoinType)
        .filter(($"df1.KpiConstants.pcpColName" === KpiConstants.yesVal))
        .select("df2.KpiConstants.memberskColName", "df2.KpiConstants.startDateSkColName")

      /*find out the member_sk based on the measure id*/
      val w15CountVisitsDf =  measureId match {

        case KpiConstants.w150MeasureId => dinominatorAfterExclusionDf.except(w15WellCareJoinDf.select(KpiConstants.memberskColName))
        case KpiConstants.w151MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(1)).select(KpiConstants.memberskColName)
        case KpiConstants.w152MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(2)).select(KpiConstants.memberskColName)
        case KpiConstants.w153MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(3)).select(KpiConstants.memberskColName)
        case KpiConstants.w154MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(4)).select(KpiConstants.memberskColName)
        case KpiConstants.w155MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(5)).select(KpiConstants.memberskColName)
        case KpiConstants.w156MeasureId => w15WellCareJoinDf.groupBy(KpiConstants.memberskColName).agg(count(KpiConstants.startDateColName).alias(KpiConstants.countColName))
          .filter($"KpiConstants.countColName".===(6)).select(KpiConstants.memberskColName)

      }

      val numeratorDf =  w15CountVisitsDf.intersect(dinominatorAfterExclusionDf)
      numeratorDf.show()

      //</editor-fold>

      //<editor-fold desc="Output creation and Store the o/p to Fact_Gaps_In_Heids Table">

      /*Common output format (data to fact_hedis_gaps_in_care)*/
      val numeratorValueSet = w15NumeratorVal
      val dinominatorExclValueSet = KpiConstants.emptyList
      val numeratorExclValueSet = KpiConstants.emptyList
      val outReasonValueSet = List(numeratorValueSet, dinominatorExclValueSet, numeratorExclValueSet)

      /*add sourcename and measure id into a list*/
      val sourceAndMsrList = List(data_source,measureId)
      val numExclDf = spark.emptyDataFrame
      val outFormatDf = UtilFunctions.commonOutputDfCreation(spark, dinominatorDf, dinominatorExclDf, numeratorDf, numExclDf, outReasonValueSet, sourceAndMsrList)
      outFormatDf.write.saveAsTable(KpiConstants.dbName+"."+KpiConstants.outFactHedisGapsInTblName)
      //</editor-fold>

      spark.sparkContext.stop()
    }

}
