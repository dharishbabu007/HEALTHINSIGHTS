package com.itc.ncqa.Constants

object KpiConstants {





  /*Common Constants*/

  val ncqaDataSource = "SELF"
  val clientDataSource = ""
  val userNameVal = "ETL"
  val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "lob","location_sk","product_plan_sk" /*,"primary_diagnosis", "procedure_code","start_date_sk", "PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
  var dbName = ""
  val yesVal =  "Y"
  val noVal  =  "N"
  val actFlgVal = "A"
  val emptyStrVal = ""
  val emptyList = List.empty[String]
  val boolTrueVal = true
  val boolFalseval = false
  val commercialLobName = "Commercial"
  val medicareLobName = "Medicare"
  val medicaidLobName = "Medicaid"

  /*function for setting the dbName with the value getting as argument */
  def setDbName(dbVal:String):String={
    dbName = dbVal
    dbName
  }

  /*age calculation constants*/
  val age18Val = "18"
  val age74Val = "74"
  val age1999Val = "19.99"
  val age2Val = "2"
  val age20Val = "20"
  val age12Val = "12"
  val age21Val = "21"
  val age75Val = "75"
  val age65Val = "65"
  val age16Val = "16"
  val age24Val = "24"
  val age67Val = "67"
  val age85Val = "85"
  val age40Val = "40"
  val age3Val = "3"
  val age6Val = "6"





  /*measurement Year Constants*/
  val measurementYearLower = 0
  val measurementOneyearUpper = 365
  val measuremetTwoYearUpper = 730




  /*Table Names*/
  val dimMemberTblName = "dim_member"
  val dimDateTblName = "dim_date"
  val dimProviderTblName = "dim_provider"
  val dimLocationTblName = "dim_location"
  val dimQltyMsrTblName = "dim_quality_measure"
  val dimFacilityTblName = "dim_facility"
  val factClaimTblName = "fact_claims"
  val factMembershipTblName = "fact_membership"
  val factRxClaimTblName = "fact_rx_claims"
  val factGapsInHedisTblName = "fact_hedis_gaps_in_care"
  val factHedisQmsTblName = "fact_hedis_qms"
  val refHedisTblName = "ref_hedis2019"
  val refLobTblName = "ref_lob"
  val refmedValueSetTblName = "ref_med_value_set"
  val view45Days = "45_days"
  val view60Days = "60_days"
  val outGapsInHedisTestTblName = "gaps_in_hedis_test"
  val outFactHedisGapsInTblName = "fact_hedis_gaps_in_care"
  val outFactQmsTblName = "fact_hedis_qms"

  /*Measure Title constants*/
  val abaMeasureTitle = "Adult BMI Assessment (ABA)"
  val advMeasureTitle = "Annual Dental Visit (ADV)"
  val awcMeasureTitle = "Adolescent WellCare Visits (AWC)"
  val cdcMeasureTitle = "Comprehensive Diabetes Care (CDC)"
  val lscMeasureTitle = "Lead Screening in Children (LSC)"
  val omwMeasureTitle = "Osteoporosis Management in Women Who Had a Fracture (OMW)"
  val spdMeasureTitle = "Statin Therapy for Patients with Diabetes (SPD)"
  val w34MeasureTitle = "WellChild Visits in the Third Fourth Fifth and Sixth Years of Life (W34)"
  val w15MeasureTitle = "WellChild Visits in the First 15 Months of Life (W15)"


  /*columnname constants*/
  val memberskColName = "member_sk"
  val lobIdColName = "lob_id"
  val lobColName = "lob"
  val dobskColame = "date_of_birth_sk"
  val dateSkColName = "date_sk"
  val calenderDateColName = "calendar_date"
  val dobColName = "dob"
  val qualityMsrSkColName = "quality_measure_sk"
  val proceedureCodeColName = "procedure_code"
  val primaryDiagnosisColname = "primary_diagnosis"
  val startDateColName = "start_date"
  val admitDateColName = "admit_date"
  val dischargeDateColName = "discharge_date"
  val endDateColName = "end_date"
  val locationSkColName = "location_sk"
  val facilitySkColName = "facility_sk"
  val providerSkColName = "provider_sk"
  val pcpColName = "pcp"
  val obgynColName = "obgyn"
  val eyeCareProvColName = "eye_care_provider"
  val nephrologistColName = "nephrologist"
  val ndcNmberColName = "ndc_number"
  val ndcCodeColName = "ndc_code"
  val measureIdColName = "measure_id"
  val startDateSkColName = "start_date_sk"
  val startTempColName = "start_temp"
  val rxStartTempColName = "rx_start_date"
  val diagStartColName = "dig_start_date"
  val iesdDateColName = "iesd_date"
  val ipsdDateColName = "ipsd_date"
  val treatmentDaysColName = "teratment_days"
  val endstrtDiffColName = "endStrt_Diff"
  val totalStatinDayColName = "totalDays_statinMed"
  val pdcColName = "pdc"


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
                                ,outDinoExcl2ReasonColName,outDinoExcl3ReasonColName,outNumExcl1ReasonColName,outNumExcl2ReasonColName,outFacilitySkColName,outCurrFlagColName
                                ,outRecCreateDateColName,outRecUpdateColName,outLatestFlagColName,outActiveFlagColName,outIngestionDateColName,outUserColName,outDateSkColName,outSourceNameColName,outMeasureIdColName)





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

  /*Join Type Constants*/
  val innerJoinType = "inner"
  val leftOuterJoinType = "left_outer"

  /*Common Queries*/
  val dimMemberLoadQuery = "select * from "+dbName+"."+dimMemberTblName
  val factClaimLoadQuery = "select * from "+dbName+"."+factClaimTblName
  val factMembershipLoadQuery = "select * from "+dbName+"."+factMembershipTblName
  val factRxClaimLoadQuery = "select * from "+dbName+"."+factRxClaimTblName
  val refHedisLoadQuery = "select * from "+dbName+"."+refHedisTblName
  val dimDateLoadQuery = "select date_sk, calendar_date from "+dbName+"."+dimDateTblName
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
  val omwMeasureId = "OMW"
  val w34MeasureId  = "W34"
  val w150MeasureId = "W150"


  /*ABA Constants*/
  val abavalueSetForDinominator = List("Outpatient")
  val abscodeSystemForDinominator = List("CPT","HCPCS","UBREV")
  val abavaluesetForDinExcl = List("Pregnancy")
  //val abacodeSytemForExcl = List("ICD%")
  val abaNumeratorBmiValueSet = List("BMI")
  val abaNumeratorBmiPercentileValueSet = List("BMI Percentile")


  /*CHL Constants*/
  val chlSexualActivityValueSet = List("Sexual Activity","Pregnancy")
  val chlDiagRadValueSet = List("Diagnostic Radiology")
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
  val cdcDiabetesvalueSet = List("Diabetes")
  val cdcNumerator1ValueSet = List("HbA1c Tests")
  val cdc1NumeratorCodeSystem = List("CPT","LOINC")

  /*CDC2 Constants*/
  val cdc2Numerator2ValueSet = List("HbA1c Level Greater Than 9.0")
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

  //val cdc4DiabetescodeSystem = List("ICD%")
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

  /*OMW Constants*/
  val omwOutPatientValueSet = List("Outpatient","Observation","ED")
  val omwOutPatientCodeSystem = List("CPT","HCPCS","UBREV")
  val omwFractureValueSet = List("Fractures")
  val omwFractureCodeSystem = List("CPT","HCPCS")
  val omwInpatientStayValueSet = List("Inpatient Stay")
  val omwInpatientStayCodeSystem = List("UBREV")
  val omwBmdTestValueSet = List("Bone Mineral Density Tests")
  val omwBmdTestCodeSystem = List("CPT","HCPCS")
  val omwOsteoprosisValueSet = List("Osteoporosis Medications")
  val omwOsteoprosisCodeSystem = List("HCPCS")
  val omwLongOsteoprosisValueSet = List("Long-Acting Osteoporosis Medications")

  /*SPDA Constants*/
  val spdaAcuteInpatientValueSet = List("Acute Inpatient")
  val spdaAcuteInpatientCodeSystem = List("CPT","UBREV")
  val spdaTeleHealthValueSet = List("Telehealth Modifier","Telehealth POS")
  val spdaTeleHealthCodeSystem = List("")
  val spdOutPatientValueSet = List("Outpatient")
  val spdOutPatientCodeSystem = List("CPT","HCPCS","UBREV")
  val spdObservationValueSet = List("Observation")
  val spdObservationCodeSystem = List("CPT")
  val spdEdVisitValueSet = List("ED")
  val spdEdVisitCodeSystem = List("CPT","UBREV")
  val spdNonAcutePatValueSet =List("Nonacute Inpatient")
  val spdNonAcutePatCodeSystem = List("CPT","UBREV")
  val spdTelephoneVisitValueSet = List("Telephone Visits")
  val spdTelephoneVisitCodeSystem = List("CPT")
  val spdOnlineAssesValueSet = List("Online Assessments")
  val spdOnlineAssesCodeSystem = List("")
  val spdMiValueSet = List("MI")
  val spdInPatStayValueSet = List("Inpatient Stay")
  val spdInPatStayCodeSystem = List("UBREV")
  val spdCabgAndPciValueSet = List("CABG","PCI","Other Revascularization")
  val spdCabgAndPciCodeSytem = List("CPT","HCPCS")
  val spdIvdValueSet = List("IVD")
  val spdPregnancyValueSet = List("Pregnancy")
  val spdIvfValueSet = List("IVF")
  val spdIvfCodeSystem = List("HCPCS")
  val spdEsrdValueSet = List("ESRD")
  val spdEsrdCodeSystem = List("CPT","HCPCS","POS","UBREV","UBTOB")
  val spdCirrhosisValueSet = List("Cirrhosis")
  val spdMusPainDisValueSet = List("Muscular Pain and Disease")
  val spdFralityValueSet = List("Frailty")
  val spdFralityCodeSystem = List("CPT", "HCPCS")
  val spdAdvancedIllValueSet = List("Advanced Illness")
  val spdDiabetesMedicationListVal = List("Diabetes Medications")
  val spdDementiaMedicationListVal = List("Dementia Medications")
  val spdEstroAgonistsMedicationListVal = List("Estrogen Agonists Medications")
  val spdHmismMedicationListVal = List("High and Moderate-Intensity Statin Medications","Low-Intensity Statin Medications")


  /*W34 Constants*/
  val w34ValueSetForNumerator =List("Well-Care")
  val w34CodeSystemForNum = List("CPT","HCPCS")

  /*W15 Constants*/
  val w15ValueSetForNumerator = List("Well-Care")
  val w15CodeSystemForNum = List("CPT","HCPCS")

}
