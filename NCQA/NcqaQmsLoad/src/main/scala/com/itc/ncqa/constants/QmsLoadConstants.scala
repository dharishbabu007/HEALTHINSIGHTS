package com.itc.ncqa.constants

object QmsLoadConstants {





  val ncqaDataSource = "SELF"
  val userNameVal = "ETL"
  val clientDataSource = ""
  val yesVal =  "Y"
  val noVal  =  "N"
  val actFlgVal = "A"
  val emptyStrVal = ""


  var db_Name = ""
  val hedis_Gaps_In_Tbl_Name = "fact_hedis_gaps_in_care"
  val factHedisQmsTableName = "fact_hedis_qms"
  val dimQltyMsrTblName = "dim_quality_measure"
  val factMembershipTblName = "fact_membership"


  val memberskColName = "member_sk"
  val facilitySkColName = "facility_sk"
  val qualityMsrSkColName = "quality_measure_sk"
  val dateSkColName = "date_sk"


  /*Join Type Constants*/
  val innerJoinType = "inner"
  val leftOuterJoinType = "left_outer"

  def setDbName(dbName:String)={
    db_Name = dbName
  }



  /*Output column names*/
  val outMemberSkColName = "member_sk"
  val outProductPlanSkColName = "product_plan_sk"
  val outQualityMeasureSkColName = "quality_measure_sk"
  val outFacilitySkColName = "facility_sk"
  val outInDinoColName = "in_denominator"
  val outInDinoExclColName = "in_denominator_exclusion"
  val outInDinoExcColName = "in_denominator_exception"
  val outInNumColName = "in_numerator"
  val outInNumExclColName = "in_numerator_exclusion"
  val outInNumExcColName = "in_numerator_exception"
  val outReasonColName = "reason"
  val outDateSkColName = "date_sk"
  val outCurrFlagColName = "curr_flag"
  val outRecCreateDateColName = "rec_create_date"
  val outRecUpdateColName = "rec_update_date"
  val outLatestFlagColName = "latest_flag"
  val outActiveFlagColName = "active_flag"
  val outIngestionDateColName = "ingestion_date"
  val outSourceNameColName = "source_name"
  val outUserColName = "user_name"
  val outNu1ReasonColName = "nu_reason_1"
  val outNu2ReasonColName = "nu_reason_2"
  val outNu3ReasonColName = "nu_reason_3"
  val outNu4ReasonColName = "nu_reason_4"
  val outNu5ReasonColName = "nu_reason_5"
  val outDinoExcl1ReasonColName = "de_ex_reason_1"
  val outDinoExcl2ReasonColName = "de_ex_reason_2"
  val outDinoExcl3ReasonColName = "de_ex_reason_3"
  val outNumExcl1ReasonColName = "nu_ex_reason_1"
  val outNumExcl2ReasonColName = "nu_ex_reason_2"
  val outHedisGapsSkColName = "hedis_gaps_in_care_sk"
  val outMeasureIdColName = "ncqa_measureid"



  /*fact_hedis_qms table column names*/
  val outHedisQmsSkColName = "hedis_qms_sk"
  val outHedisQmsLobidColName = "lob_id"
  val outHedisQmsDinominatorColName = "denominator"
  val outHedisQmsNumeratorColName = "numerator"
  val outHedisQmsRatioColName = "ratio"
  val outHedisQmsNumExclColName = "num_exclusion"
  val outHedisQmsDinoExclColName = "deno_exclusion"
  val outHedisQmsNumExcColName = "num_exception"
  val outHedisQmsDinoExcColName = "deno_exception"
  val outHedisQmsPerformanceColName = "performance"
  val outHedisQmsBonusColName = "bonus"


  /*order of columns in fact_hedis_qms*/
  val outFactHedisQmsFormattedList = List(outHedisQmsSkColName,outQualityMeasureSkColName,outDateSkColName,outFacilitySkColName,outHedisQmsLobidColName,outProductPlanSkColName,outHedisQmsNumeratorColName,outHedisQmsDinominatorColName,outHedisQmsRatioColName,
    outHedisQmsNumExclColName,outHedisQmsDinoExclColName,outHedisQmsNumExcColName,outHedisQmsDinoExcColName,outHedisQmsPerformanceColName,outHedisQmsBonusColName,outCurrFlagColName,outRecCreateDateColName,outRecUpdateColName,
    outLatestFlagColName,outActiveFlagColName,outIngestionDateColName,outSourceNameColName,outUserColName)
}
