package com.itc.ncqa.Constants

object OutputCreateConstants {



  /*common constants*/
  val yesVal = "Y"
  val noVal ="N"
  val yesBoolVal = true
  val noBoolVal = false
  val oneVal = 1
  val zeroVal = 0


  /*Measure Title constants*/
  val abaMeasureTitle = "Adult BMI Assessment (ABA)"

  /*Datbase Name*/
  val dbName = "ncqa_sample"

  /*Join type Names*/
  val innerJoinType = "inner"
  val leftOuterJoinType = "left_outer"


  /*Table Names*/
  val dimQltyMsrTblName = "dim_quality_measure"
  val gapsInHedisTblName = "gaps_in_hedis_test"
  val dimMemberTblName = "dim_member"
  val dimLobTblName = "ref_lob"
  val factMembershipTblName = "fact_membership"



  /*Common Column Names*/
  val qultyMsrSkColName = "quality_measure_sk"
  val memberSkColName = "member_sk"
  val memberIdColName = "member_id"
  val lobIdColName = "lob_id"
  val lobNameColName = "lob_name"
  val productPlanSkColName = "product_plan_sk"
  val ncqaMeasureIdColName = "ncqa_measureid"


  /*NCQA outputformat Column Names*/
  val ncqaOutmemberIdCol = "MemID"
  val ncqaOutMeasureCol = "Meas"
  val ncqaOutPayerCol = "Payer"
  val ncqaOutEpopCol = "Epop"
  val ncqaOutExclCol = "Excl"
  val ncqaOutNumCol = "Num"
  val ncqaOutRexclCol ="RExcl"
  val ncqaOutIndCol = "Ind"

  /*Ncqa Output format column order*/
  val outColsFormatList = List(ncqaOutmemberIdCol,ncqaOutMeasureCol,ncqaOutPayerCol,ncqaOutEpopCol,ncqaOutExclCol,ncqaOutNumCol,ncqaOutRexclCol,ncqaOutIndCol)


}
