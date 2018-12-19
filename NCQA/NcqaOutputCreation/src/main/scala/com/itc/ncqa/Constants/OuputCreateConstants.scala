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
  val lobColName = "lob"
  val productPlanSkColName = "product_plan_sk"
  val ncqaMeasureIdColName = "ncqa_measureid"
  val ncqaAgeColName = "age"


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


  /*Measure id constants*/
  val emptyMesureId = ""
  val abaMeasureId = "ABA"
  val advMeasureId = "ADV"
  val awcMeasureId = "AWC"
  val cdcMeasureId = "CDC"
  val cdc1MeasureId = "CDC1"
  val cdc2MeasureId = "CDC2"
  val cdc3MeasureId = "CDC3"
  val cdc4MeasureId = "CDC4"
  val cdc7MeasureId = "CDC7"
  val cdc9MeasureId = "CDC9"
  val cdc10MeasureId = "CDC10"
  val chlMeasureId = "CHL"
  val lsMeasureId  = "LSC"
  val spdMeasureId = "SPD"
  val spdaMeasureId = "SPDA"
  val spdbMeasureId = "SPDB"
  val spcMeasureId = "SPC"
  val spc1aMeasureId = "SPC1A"
  val spc2aMeasureId = "SPC2A"
  val spc1bMeasureId = "SPC1B"
  val spc2bMeasureId = "SPC2B"
  val omwMeasureId = "OMW"
  val w34MeasureId  = "W34"
  val w150MeasureId = "W150"
  val w151MeasureId = "W151"
  val w152MeasureId = "W152"
  val w153MeasureId = "W153"
  val w154MeasureId = "W154"
  val w155MeasureId = "W155"
  val w156MeasureId = "W156"
  val cisDtpaMeasureId = "CISDTP"
  val cisIpvMeasureId = "CISIPV"
  val cisHiBMeasureId = "CISHIB"
  val cisPneuMeasureId = "CISPNEU"
  val cisInflMeasureId = "CISINFL"
  val cisRotaMeasureId = "CISROTA"
  val cisMmrMeasureId = "CISMMR"
  val cisHepbMeasureId = "CISHEPB"
  val cisVzvMeasureId = "CISVZV"
  val cisHepaMeasureId = "CISHEPA"
  val cisCmb10MeasureId = "CISCMB10"
  val cisCmb9MeasureId = "CISCMB9"
  val cisCmb8MeasureId = "CISCMB8"
  val cisCmb7MeasureId = "CISCMB7"
  val cisCmb6MeasureId = "CISCMB6"
  val cisCmb5MeasureId = "CISCMB5"
  val cisCmb4MeasureId = "CISCMB4"
  val cisCmb3MeasureId = "CISCMB3"
  val cisCmb2MeasureId = "CISCMB2"
  val colMesureId = "COL"
  val bcsMeasureId = "BCS"
  val ImamenMeasureId = "IMAMEN"
  val ImatdMeasureId = "IMATD"
  val ImahpvMeasureId = "IMAHPV"
  val Imacmb1MeasureId = "IMACMB1"
  val Imacmb2MeasureId = "IMACMB2"
  val aisMeasureId = "AIS"
  val aisf1MeasureId = "AISINFL1"
  val aisf2MeasureId = "AISINFL2"
  val aistd1MeasureId = "AISTD1"
  val aistd2MeasureId = "AISTD2"
  val aiszos1MeasureIdVal = "AISZOS1"
  val aiszos2MeasureIdVal = "AISZOS2"
  val wcc1aMeasureId = "WCC1A"
  val wcc2aMeasureId = "WCC2A"
  val wcc1bMeasureId = "WCC1B"
  val wcc2bMeasureId = "WCC2B"
  val wcc1cMeasureId = "WCC1C"
  val wcc2cMeasureId = "WCC2C"
  val aap1MeasureId = "AAP1"
  val aap2MeasureId = "AAP2"
  val aap3MeasureId = "AAP3"
  val capMeasureId = "CAP"
  val cap1MeasureId = "CAP1"
  val cbpMeasureId = "CBP"
}
