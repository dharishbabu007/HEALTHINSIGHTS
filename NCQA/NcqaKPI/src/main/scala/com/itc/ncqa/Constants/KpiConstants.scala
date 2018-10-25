package com.itc.ncqa.Constants

object KpiConstants {


  /*Common Constants*/
  val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "primary_diagnosis", "procedure_code", "start_date_sk" /*"PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
  val dbName = "ncqa_sample"
  //val dbName = "healthin"
  val dimMemberTblName = "dim_member"
  val factClaimTblName = "fact_claims"
  val factMembershipTblName = "fact_membership"
  val factRxClaimTblName = "fact_rx_claims"
  val refHedisTblName = "ref_hedis2016"
  val dimDateTblName = "dim_date"
  val dimProviderTblName = "dim_provider"
  val refLobTblName = "ref_lob"
  val refmedValueSetTblName = "ref_med_value_set"
  val view45Days = "45_days"
  val view60Days = "60_days"



  /*Common Queries*/
  val dimMemberLoadQuery = "select * from "+dbName+"."+dimMemberTblName
  val factClaimLoadQuery = "select * from "+dbName+"."+factClaimTblName
  val factMembershipLoadQuery = "select * from "+dbName+"."+factMembershipTblName
  val factRxClaimLoadQuery = "select * from "+dbName+"."+factRxClaimTblName
  val refHedisLoadQuery = "select * from "+dbName+"."+refHedisTblName
  val dimDateLoadQuery = "select * from "+dbName+"."+dimDateTblName
  val refLobLoadQuery =  "select * from "+dbName+"."+refLobTblName
  val dimProviderLoadQuery = "select * from "+dbName+"."+dimProviderTblName
  val refmedvaluesetLoadQuery = "select * from "+dbName+"."+refmedValueSetTblName
  val view45DaysLoadQuery = "select * from "+dbName+"."+view45Days
  val view60DaysLoadQuery = "select * from "+dbName+"."+view60Days



  /*common kpi Constants*/
  val primaryDiagnosisCodeSystem = List("ICD%")

  /*Measure id constants*/
  val abaMeasureId = "ABA"
  val advMeasureId = "ADV"
  val awcMeasureId = "AWC"
  val cdcMeasureId = "CDC"
  val chlMeasureId = "CHL"
  val lsMeasureId =   "LSC"
  val spdMeasureId = "SPD"


  /*ABA Constants*/
  val abavalueSetForDinominator = List("Outpatient")
  val abscodeSystemForDinominator = List("CPT","HCPCS","UBREV")
  val abavaluesetForDinExcl = List("Pregnancy")
  val abacodeSytemForExcl = List("ICD%")
  val abanumeratorValueSet = List("BMI","BMI Percentile")


  /*CHL Constants*/
  val chlSexualActivityValueSet = List("Sexual Activity","Pregnancy")
  val chlSexualActivitycodeSytem = List("ICD%")
  val chlpregnancyValueSet = List("Pregnancy Tests")
  val chlpregnancycodeSystem = List("CPT","HCPCS","UBREV")
  val chlPregnancyExclusionvalueSet = List("Pregnancy Test Exclusion")
  val chlPregnancyExclusioncodeSystem = List("CPT","HCPCS","UBREV")
  val chlchalmdiaValueSet = List("Chlamydia Tests")
  val chlChalmdiacodeSystem = List("CPT","HCPS","LOINC")




  /*CLS Constants*/
  val clsValueSetForNumerator =List("Lead Tests")
  val clsCodeSystemForNum = List("CPT","LOINC","HCPCS")


  /*CDC1 Constants*/
  val cdc1NumeratorValueSet = List("HbA1c Tests")
  val cdc1NumeratorCodeSystem = List("CPT","LOINC")

  /*CDC2 Constants*/
  val cdc2NumeratorValueSet = List("HbA1c Level Greater Than 9.0")
  val cdc2NumeratorCodeSystem = List("CPT")

  /*CDC3 constants*/
  val cdc3NumeratorValueSet = List("HbA1c Level Less Than 7.0")
  val cdc3CabgValueSet = List("CABG")
  val cdc3PciValueSet = List("PCI")
  val cdc3OutPatientValueSet = List("Outpatient")
  val cdc3OutPatientCodeSystem = List("CPT","HCPCS","UBREV")
  val cdc3IvdExclValueSet = List("IVD")
  val cdc3AccuteInPtValueSet =List("Acute Inpatient")
  val cdc3AccuteInPtCodeSystem =List("CPT","UBREV")
  val cdc3ThAoAnvalueSet = List("Thoracic Aortic Aneurysm")
  val cdc3ChfExclValueSet = List("Chronic Heart Failure")
  val cdc3MiExclValueSet = List("MI")
  val cdc3EsrdExclValueSet = List("ESRD","ESRD Obsolete")
  val cdc3EsrdExclcodeSystem = List("CPT","HCPCS","POS","UBREV","UBTOB")
  val cdc3CkdStage4ValueSet = List("CKD Stage 4")
  val cdc3DementiaExclValueSet = List("Dementia","Frontotemporal Dementia")
  val cdc3BlindnessExclValueSet = List("Blindness")
  val cdc3LEAExclValueSet = List("Lower Extremity Amputation")
  val cdc3LEAExclCodeSystem = List("CPT")




  /*CDC4 Constants*/
  val cdc4DiabetesvalueSet = List("Diabetes")
  val cdc4DiabetescodeSystem = List("ICD%")
  val cdc4ValueSetForFirstNumerator = List("Diabetic Retinal Screening")
  val cdc4CodeSystemForFirstNumerator = List("CPT","HCPCS")
  val cdc4eyeCareListForFirstNumerator = List("optometrist","ophthalmologist")
  val cdc4ValueSetForThirdNumerator=List("Diabetes Mellitus without Complications")
  val cdc4ValueSetForFourthNumerator = List("Diabetic Retinal Screening with Eye Care Professional")
  val cdc4CodeSystemForFourthNumerator = List("")
  val cdc4ValueSetForSixthNumerator = List("Diabetic Retinal Screening Negative")
  val cdc4ValueSetForUnilateralEyeUnicleation = List("Unilateral Eye Enucleation")
  val cdc4ValueSetForBilateral = List("Bilateral Modifier")
  val cdc4UnilateralEyeEnuLeftValueSet = List("Unilateral Eye Enucleation Left")
  val cdc4UnilateralEyeEnuRightValueSet = List("Unilateral Eye Enucleation Right")
  val cdcDiabetesExclValueSet = List("Diabetes Exclusions")



  /*CDC7 Constants*/
  val cdc7uptValueSet = List("Urine Protein Tests")
  val cdc7uptCodeSystem = List("CPT","LOINC")
  val cdc7NtValueSet = List("Nephropathy Treatment")
  val cdc7NtCodeSystem = List("CPT")
  val cdc7EsrdValueSet = List("ESRD")
  val cdc7KtValueSet = List("Kidney Transplant")
  val cdc7KtCodeSystem = List("CPT","HCPCS","UBREV")

  /*CDC9 Constants*/
  val cdc9SystolicValueSet = List("Systolic Less Than 140")
  val cdc9SystolicAndDiastolicCodeSystem = List("CPT")
  val cdc9DiastolicValueSet = List("Diastolic Less Than 80","Diastolic 80–89")
  //val cdc9DiastolicBtwn80A90ValueSet = List("Diastolic 80–89")

  /*CDC10 Constants*/
  val cdc10Hba1cValueSet = List("HbA1c Level Less Than 7.0")
  val cdc10Hba1cCodeSystem = List("CPT")

  /*ADV Constants*/
  val advOutpatientValueSet = List("Dental Visits")
  val advOutpatientCodeSystem = List("CPT","HCPCS")

  /*AWC Constants*/
  val awcWcvValueSet = List("Well-Care")
  val awcWcvCodeSystem = List("CPT","HCPCS")



}
