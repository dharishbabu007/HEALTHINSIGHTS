package com.itc.ncqa.constants

object NcqaGictlConstants {



  val ncqaDataSource = "SELF"
  val userNameVal = "ETL"
  val clientDataSource = ""


  var db_name = ""
  val factHedisGapsInCareTaleName = "fact_hedis_gaps_in_care"
  val factHedisGapsInCareTmpTblName = "fact_hedis_gaps_in_care_temp"
  val dimMemberTableName = "dim_member"
  val factMembershipTblName = "fact_membership"
  val dimDateTblName = "dim_date"
  val refLobTblName = "ref_lob"

  /*column name constants*/
  val memberskColName = "member_sk"
  val productPlanSkColName = "product_plan_sk"
  val memberidColName = "member_id"
  val lobIdColName = "lob_id"
  val lobColName = "lob"
  val lobNameColName = "lob_name"
  val lobDescColName = "lob_desc"
  val dobskColame = "date_of_birth_sk"
  val dateSkColName = "date_sk"
  val calenderDateColName = "calendar_date"
  val dobColName = "dob"
  val ageColName = "age"




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

  /*order of the column in fact_gaps_in_hedis*/
  val outFormattedArray = Array(outHedisGapsSkColName,outMemberSkColName,outProductPlanSkColName,outQualityMeasureSkColName,outInDinoColName,outInDinoExclColName,outInDinoExcColName,outInNumColName
    ,outInNumExclColName,outInNumExcColName,outNu1ReasonColName,outNu2ReasonColName,outNu3ReasonColName,outNu4ReasonColName,outNu5ReasonColName,outDinoExcl1ReasonColName
    ,outDinoExcl2ReasonColName,outDinoExcl3ReasonColName,outNumExcl1ReasonColName,outNumExcl2ReasonColName,outFacilitySkColName,outDateSkColName,outSourceNameColName,outMeasureIdColName,outCurrFlagColName
    ,outRecCreateDateColName,outRecUpdateColName,outLatestFlagColName,outActiveFlagColName,outIngestionDateColName,outUserColName)



  /*Join Type Constants*/
  val innerJoinType = "inner"
  val leftOuterJoinType = "left_outer"


  def setDbName (dbName:String) ={
    db_name = dbName
  }
}
