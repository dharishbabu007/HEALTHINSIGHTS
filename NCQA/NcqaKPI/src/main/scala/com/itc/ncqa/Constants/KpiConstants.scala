package com.itc.ncqa.Constants

object KpiConstants {





  /*Common Constants*/

  val ncqaDataSource = "SELF"
  val clientDataSource = ""
  val userNameVal = "ETL"
  val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "lob","location_sk","product_plan_sk","member_plan_start_date_sk","member_plan_end_date_sk" /*,"primary_diagnosis", "procedure_code","start_date_sk", "PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
  val arrayofColumn1 = List("member_id" , "state" , "city" , "member_plan_start_date_sk" , "member_plan_end_date_sk","lob_id")
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
  val age1Val = "1"
  val age2Val = "2"
  val age20Val = "20"
  val age12Val = "12"
  val age19Val = "19"
  val age21Val = "21"
  val age75Val = "75"
  val age65Val = "65"
  val age66Val = "66"
  val age16Val = "16"
  val age24Val = "24"
  val age67Val = "67"
  val age85Val = "85"
  val age40Val = "40"
  val age3Val = "3"
  val age6Val = "6"
  val age7Val = "7"
  val age51Val = "51"
  val age52Val = "52"
  val age11Val = "11"
  val age13Val = "13"
  val age10val = "10"
  val age9Val = "9"
  val age0Val = "0"
  val age17Val = "17"
  val age44Val = "44"
  val age45Val = "45"
  val age64Val = "64"
<<<<<<< HEAD
  val age81Val = "81"
=======
<<<<<<< HEAD
=======
  val age81Val = "81"
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val age120Val = "120"





  /*measurement Year Constants*/
  val measurementYearLower = 0
  val measurementOneyearUpper = 365
  val measuremetTwoYearUpper = 730
  val measurementFourYearUpper = 1460
  val measurementNineYearUpper = 3287
  val measurementThreeYearUpper = 1096
  val measureemtnTenYearUpper = 3650
<<<<<<< HEAD
  val measurement0Val = 0
  val measurement1Val = 1
  val measurement2Val = 2
  val measurement3Val = 3
  val measurement4Val = 4
  val measurement9Val = 9
  val measurement10Val = 10
=======
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753




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
  val chlMeasureTitle = "Chlamydia Screening in Women"
  val advMeasureTitle = "Annual Dental Visit (ADV)"
  val awcMeasureTitle = "Adolescent WellCare Visits (AWC)"
  val cdcMeasureTitle = "Comprehensive Diabetes Care (CDC)"
  val lscMeasureTitle = "Lead Screening in Children (LSC)"
  val omwMeasureTitle = "Osteoporosis Management in Women Who Had a Fracture (OMW)"
  val spdMeasureTitle = "Statin Therapy for Patients with Diabetes (SPD)"
  val spcMeasureTitle = "Statin Therapy for Patients with Cardiovascular Disease (SPC)"
  val w34MeasureTitle = "WellChild Visits in the Third Fourth Fifth and Sixth Years of Life (W34)"
  val w15MeasureTitle = "WellChild Visits in the First 15 Months of Life (W15)"
  val cisMeasureTitle = "Childhood Immuniztions Status (CIS)"
  val colMeasureTitle = "Colorectal Cancer Screening (COL)"
  val bcsMeasureTitle = "Breast Cancer Screening (BCS)"
  val ImamenMeasureTitle = "Immunizations for Adolescents (IMA)"
  val WCCMeasureTitle = "Weight Assessment and Counseling for Nutrition and Physical Activity for Children/Adolescent"
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val aapMeasureTitle = "Adult Access to Preventive/Ambulatory Health Services (AAP)"
  val aisMeasureTitle = "Adult Immunization Status (AIS)"
  val capMeasureTitle = "Children and Adolescents Access to Primary Care Practitioners (CAP)"
  val cbpMeasureTitle = "Controlling High Blood Pressure (CBP)"

<<<<<<< HEAD
=======
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753



  /*columnname constants*/
  val memberskColName = "member_sk"
  val memberidColName = "member_id"
  val lobIdColName = "lob_id"
  val lobColName = "lob"
  val lobNameColName = "lob_name"
  val dobskColame = "date_of_birth_sk"
  val dateSkColName = "date_sk"
  val calenderDateColName = "calendar_date"
  val dobColName = "dob"
  val memStartDateColName = "mem_start_date"
  val memEndDateColName = "mem_end_date"
  val genderColName = "gender"
  val stateColName = "state"
  val qualityMsrSkColName = "quality_measure_sk"
  val proceedureCodeColName = "procedure_code"
  val primaryDiagnosisColname = "primary_diagnosis"
  val memPlanStartDateSkColName = "member_plan_start_date_sk"
  val memPlanEndDateSkColName = "member_plan_end_date_sk"
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
  val fiftyDobColName = "fifty_dob"
  val sixtyDobColName = "sixty_dob"
  val countColName = "count"


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
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val aisf1MeasureId = "AISINFL1"
  val aisf2MeasureId = "AISINFL2"
  val aistd1MeasureId = "AISTD1"
  val aistd2MeasureId = "AISTD2"
  val aiszos1MeasureIdVal = "AISZOS1"
  val aiszos2MeasureIdVal = "AISZOS2"
<<<<<<< HEAD
=======
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val wcc1aMeasureId = "WCC1A"
  val wcc2aMeasureId = "WCC2A"
  val wcc1bMeasureId = "WCC1B"
  val wcc2bMeasureId = "WCC2B"
  val wcc1cMeasureId = "WCC1C"
  val wcc2cMeasureId = "WCC2C"
  val aap1MeasureId = "AAP1"
  val aap2MeasureId = "AAP2"
  val aap3MeasureId = "AAP3"
<<<<<<< HEAD
  val capMeasureId = "CAP"
  val cap1MeasureId = "CAP1"
  val cbpMeasureId = "CBP"
=======
<<<<<<< HEAD
=======
  val capMeasureId = "CAP"
  val cap1MeasureId = "CAP1"
  val cbpMeasureId = "CBP"
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753



  /*Valueset Constants*/
  val outPatientVal = "Outpatient"
  val pregnancyVal = "Pregnancy"
  val bmiVal = "BMI"
  val bmiPercentileVal = "BMI Percentile"
  val sexualActivityVal = "Sexual Activity"
  val diagnosticRadVal = "Diagnostic Radiology"
  val pregnancyTestVal = "Pregnancy Tests"
  val pregnancyExcltestVal = "Pregnancy Test Exclusion"
  val chalamdiaVal = "Chlamydia Tests"
  val leadTestVal = "Lead Tests"
  val diabetesVal = "Diabetes"
  val hba1cTestVal = "HbA1c Tests"
  val hba1cGtNineVal = "HbA1c Level Greater Than 9.0"
  val hba1cLtSevenVal = "HbA1c Level Less Than 7.0"
  val cabgVal = "CABG"
  val pctVal = "PCI"
  val ivdVal = "IVD"
  val accuteInpatVal = "Acute Inpatient"
  val thoraticAcriticVal = "Thoracic Aortic Aneurysm"
  val chronicHeartFailureVal = "Chronic Heart Failure"
  val miVal = "MI"
  val esrdVal = "ESRD"
  val esrdObsoleteVal = "ESRD Obsolete"
  val ckdStage4Val = "CKD Stage 4"
  val dementiaVal = "Dementia"
  val fronDementiaVal = "Frontotemporal Dementia"
  val blindnessVal = "Blindness"
  val lowerExtrAmputationVal = "Lower Extremity Amputation"
  val diabeticRetinalScreeningVal = "Diabetic Retinal Screening"
  val optometristVal = "optometrist"
  val opthoalmologistVal = "ophthalmologist"
  val diabtesMellWoCompliVal = "Diabetes Mellitus without Complications"
  val diabeticReScreeWECProfessionalVal = "Diabetic Retinal Screening with Eye Care Professional"
  val diabeticRenScreNegativeVal = "Diabetic Retinal Screening Negative"
  val unilateralEyeEnucleationVal = "Unilateral Eye Enucleation"
  val unilateralEyeEnuLeftVal = "Unilateral Eye Enucleation Left"
  val unilateralEyeEnuRightVal = "Unilateral Eye Enucleation Right"
  val diabetesExclusionVal = "Diabetes Exclusions"
  val urineProteinTestVal = "Urine Protein Tests"
  val nephropathyTreatmentVal= "Nephropathy Treatment"
  val kidneyTransplantVal = "Kidney Transplant"
  val systolicLt140Val = "Systolic Less Than 140"
  val diastolicLt80Val = "Diastolic Less Than 80"
  val diastolicBtwn8090Val = "Diastolic 80–89"
  val dentalVisitsVal = "Dental Visits"
  val wellCareVal= "Well-Care"
  val observationVal = "Observation"
  val edVal = "ED"
  val fracturesVal = "Fractures"
  val inpatientStayVal = "Inpatient Stay"
  val boneMinDenTestVal = "Bone Mineral Density Tests"
  val osteoporosisMedicationVal  = "Osteoporosis Medications"
  val longActingOsteoMedicationVal = "Long-Acting Osteoporosis Medications"
  val acuteInpatientVal = "Acute Inpatient"
  val telehealthModifierVal = "Telehealth Modifier"
  val telehealthPosVal = "Telehealth POS"
  val nonAcuteInPatientVal = "Nonacute Inpatient"
  val telephoneVisitsVal = "Telephone Visits"
  val onlineAssesmentVal = "Online Assessments"
  val inPatientStayVal = "Inpatient Stay"
  val pciVal = "PCI"
  val otherRevascularizationVal = "Other Revascularization"
  val ivfVal = "IVF"
  val cirrhossisVal = "Cirrhosis"
  val muscularPainAndDiseaseval = "Muscular Pain and Disease"
  val fralityVal = "Frailty"
  val advancedIllVal = "Advanced Illness"
  val diabetesMedicationVal = "Diabetes Medications"
  val dementiaMedicationVal = "Dementia Medications"
  val estrogenAgonistsMediVal = "Estrogen Agonists Medications"
  val highAndModerateStatinMedVal = "High and Moderate-Intensity Statin Medications"
  val lowStatinMedVal = "Low-Intensity Statin Medications"
  val fobtVal = "FOBT"
  val flexibleSigmodoscopyVal = "Flexible Sigmoidoscopy"
  val colonoscopyVal = "Colonoscopy"
  val ctColonographyVal = "CT Colonography"
  val fitDnaVal = "FIT-DNA"
  val colorectalCancerVal = "Colorectal Cancer"
  val totalColectomyVal = "Total Colectomy"
  val bilateralMastectomyVal = "Bilateral Mastectomy"
  val unilateralMastectomyVal = "Unilateral Mastectomy"
  val bilateralModifierVal = "Bilateral Modifier"
  val historyBilateralMastectomyVal = "History of Bilateral Mastectomy"
  val leftModifierVal = "Left Modifier"
  val rightModifierVal = "Right Modifier"
  val absOfLeftBreastVal = "Absence of Left Breast"
  val absOfRightBreastVal = "Absence of Right Breast"
  val uniMasLeftVal = "Unilateral Mastectomy Left"
  val uniMasRightVal = "Unilateral Mastectomy Right"
  val ardvVal = "Anaphylactic Reaction Due To Vaccination"
  val encephalopathyVal = "Encephalopathy Due To Vaccination"
<<<<<<< HEAD
  val tdVaccineVal = "Td Vaccine"
=======
<<<<<<< HEAD
=======
  val tdVaccineVal = "Td Vaccine"
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val tdapVaccineVal = "Tdap Vaccine"
  val boneMarowTransVal = "Bone Marrow Transplant"
  val chemoTherappyVal = "Chemotherapy"
  val immunoCompromisingVal = "Immunocompromising Conditions"
  val cochlearImplantVal = "Cochlear Implant"
  val afaVal = "Anatomic or Functional Asplenia"
  val scaHbsdVal = "Sickle Cell Anemia and HB-S Disease"
  val cflVal = "Cerebrospinal Fluid Leak"
  val hospiceVal = "Hospice"
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
  val influenzaVaccineVal = "Influenza Vaccine Administered"
  val herpesZosterLiveVaccineVal = "Herpes Zoster Live Vaccine"
  val herpesZosterRecomVaccineVal = "Herpes Zoster Recombinant Vaccine"
  val pneuConjuVaccine13Val = "Pneumococcal Conjugate Vaccine 13"
  val pneuPolyVaccine23Val = "Pneumococcal Polysaccharide Vaccine 23"
  val ambulatoryVisitVal = "Ambulatory Visits"
<<<<<<< HEAD
=======
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753


























  /*Codesystem constants*/
  val icdCodeVal = "ICD%"
  val cptCodeVal = "CPT"
  val hcpsCodeVal = "HCPCS"
  val ubrevCodeVal = "UBREV"
  val loincCodeVal = "LOINC"
  val modifierCodeVal = "Modifier"
  val cvxCodeVal = "CVX"
  val snomedctCodeVal = "SNOMED CT US Edition"



  /*ABA Constants*/
  val abavalueSetForDinominator = List("Outpatient")
  val abscodeSystemForDinominator = List("CPT","HCPCS","UBREV")
  val abavaluesetForDinExcl = List("Pregnancy")
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

  /*SPC constants*/
  val spcAcuteInpatientValueSet = List("Acute Inpatient")
  val spcAcuteInpatientCodeSystem = List("CPT","UBREV")
  val spcTeleHealthValueSet = List("Telehealth Modifier","Telehealth POS")
  val spcTeleHealthCodeSystem = List("")
  val spcOutPatientValueSet = List("Outpatient")
  val spcOutPatientCodeSystem = List("CPT","HCPCS","UBREV")
  val spcObservationValueSet = List("Observation")
  val spcObservationCodeSystem = List("CPT")
  val spcEdVisitValueSet = List("ED")
  val spcEdVisitCodeSystem = List("CPT","UBREV")
  val spcNonAcutePatValueSet =List("Nonacute Inpatient")
  val spcNonAcutePatCodeSystem = List("CPT","UBREV")
  val spcTelephoneVisitValueSet = List("Telephone Visits")
  val spcTelephoneVisitCodeSystem = List("CPT")
  val spcOnlineAssesValueSet = List("Online Assessments")
  val spcOnlineAssesCodeSystem = List("")
  val spcMiValueSet = List("MI")
  val spcInPatStayValueSet = List("Inpatient Stay")
  val spcInPatStayCodeSystem = List("UBREV")
  val spcCabgAndPciValueSet = List("CABG","PCI","Other Revascularization")
  val spcCabgAndPciCodeSytem = List("CPT","HCPCS")
  val spcIvdValueSet = List("IVD")
  val spcPregnancyValueSet = List("Pregnancy")
  val spcIvfValueSet = List("IVF")
  val spcIvfCodeSystem = List("HCPCS")
  val spcEsrdValueSet = List("ESRD")
  val spcEsrdCodeSystem = List("CPT","HCPCS","POS","UBREV","UBTOB")
  val spcCirrhosisValueSet = List("Cirrhosis")
  val spcMusPainDisValueSet = List("Muscular Pain and Disease")
  val spcFralityValueSet = List("Frailty")
  val spcFralityCodeSystem = List("CPT", "HCPCS")
  val spcAdvancedIllValueSet = List("Advanced Illness")
  val spcDiabetesMedicationListVal = List("Diabetes Medications")
  val spcDementiaMedicationListVal = List("Dementia Medications")
  val spcEstroAgonistsMedicationListVal = List("Estrogen Agonists Medications")
  val spcHmismMedicationListVal = List("High and Moderate-Intensity Statin Medications")


  /*W34 Constants*/
  val w34ValueSetForNumerator =List("Well-Care")
  val w34CodeSystemForNum = List("CPT","HCPCS")

  /*W15 Constants*/
  val w15ValueSetForNumerator = List("Well-Care")
  val w15CodeSystemForNum = List("CPT","HCPCS")


  /*CISDTaP constants added by Thanuja*/

  val cisDtpaValueSet = List("DTaP Vaccine Administered ")
  val cisDtpaCodeSystem = List("CPT")


  val cisDtpaExclValuSet = List("Anaphylactic Reaction Due To Vaccination","Encephalopathy Due To Vaccination","Vaccine Causing Adverse Effect")

  /*CISIPV constants added by Thanuja*/

  val cisIpvValueSet = List("Inactivated Polio Vaccine (IPV) Administered")
  val cisIpvCodeSystem = List("CPT", "CVX")

  /*CISHIB constants added by Thanuja*/

  val cisHiBValueSet = List("Haemophilus Influenzae Type B (HiB) Vaccine Administered")
  val cisHiBCodeSystem = List("CPT", "CVX")



  /*CIS constants added by Thanuja*/

  val cisPneuValueSet = List("Pneumococcal Conjugate Vaccine Administered")
  val cisPneuCodeSystem = List("CPT", "CVX", "HCPCS")

  /*CIS constants added by Thanuja*/

  val cisRota1ValueSet = List("Rotavirus Vaccine (2 Dose Schedule) Administered")
  val cisRotaCodeSystem = List("CPT", "CVX")
  val cisRota2ValueSet = List("Rotavirus Vaccine (3 Dose Schedule) Administered")
  val cisRotaAllValueSet = List("Rotavirus Vaccine (2 Dose Schedule) Administered","Rotavirus Vaccine (3 Dose Schedule) Administered")

  /*CIS constants added by Thanuja*/

  val cisInflValueSet = List("Influenza Vaccine Administered")
  val cisInflCodeSystem = List("CPT", "CVX", "HCPCS")

  /*CIS MMR constants added by Thanuja*/

  val cisMmrValueSet = List("Measles/Rubella Vaccine Administered","Mumps Vaccine Administered","Mumps","Measles Vaccine Administered","Measles","Rubella Vaccine Administered","Rubella","Mumps and Rubella (MMR) Vaccine Administered")
  val cisMmrCodeSystem = List("CPT", "CVX")

  /*CIS Hepb constants added by Thanuja*/

  val cisHepbValueSet = List("Hepatitis B Vaccine Administered","Newborn Hepatitis B Vaccine Administered","Hepatitis B")
  val cisHepbCodeSystem = List("CPT", "CVX")


  /*CIS VZV constants added by Thanuja*/

  val cisVzvValueSet = List("Varicella Zoster (VZV) Vaccine Administered","Varicella Zoster")
  val cisVzvCodeSystem = List("CPT", "CVX")

  /*CIS Hepa constants added by Thanuja*/

  val cisHepaValueSet = List("Hepatitis A Vaccine Administered","Hepatitis A")
  val cisHepaCodeSystem = List("CPT", "CVX")

  val cisMmrVzvInflDinoExclValueSet =  List("Disorders of the Immune System","Encephalopathy Due To Vaccination","HIV","HIV Type 2","Malignant Neoplasm of Lymphatic Tissue","Anaphylactic Reaction Due To Vaccination")

  val cisRotaDinoExclValueSet = List("Severe Combined Immunodeficiency","Intussusception","Anaphylactic Reaction Due To Vaccination")


  /*added by Thanuja2*/

  /*CIS IMAMEN constants added by Thanuja*/

  val cisImamenValueSet = List("Meningococcal Vaccine Administered")
  val cisImamenCodeSystem = List("CPT", "CVX")

  val cisImatdValueSet = List("Tdap Vaccine Administered")
  val cisImatdCodeSystem = List("CPT", "CVX")


  val cisImahpvValueSet = List("HPV Vaccine Administered")
  val cisImahpvCodeSystem = List("CPT", "CVX")

  val cisImamenDinoExclValueSet = List("Anaphylactic Reaction Due To Vaccination","Anaphylactic Reaction Due To Serum")


  val wcc1bNutritionValueSet = List("Nutrition Counseling")

  val wcc1bNutritionCodeSystem = List("CPT", "HCPCS")

  val wcc1cPhysicalValueSet = List("Physical Activity Counseling")

  val wcc1cPhysicalCodeSystem = List("HCPCS")

  val aapValueSet = List("CPT","HCPCS","UBREV","Modifier")

  val aapCodeSystem = List("Ambulatory Visits","Telehealth Modifier","Other Ambulatory Visits","Telephone Visits","Online Assessments")
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753

  val cbpDinoExclValueSet = List("Frailty")

  val cbpDinoCodeSystem = List("CPT", "HCPCS")

  val cbpCommonNumeratorValueSet = List("Essential Hypertension")

  val cbpNumerator1ValueSet = List("Outpatient","Telehealth Modifier")

  val cbpNumerator2ValueSet = List("Telephone Visits")

  val cbpNumerator3ValueSet = List("Online Assessments")

  val cbpNumerator1CodeSystem = List("CPT","HCPCS","Modifier")

  val cbpNumerator2CodeSystem = List("CPT")

  val cbpNumerator3CodeSystem = List("CPT")

<<<<<<< HEAD
=======
>>>>>>> origin/sangeeth
>>>>>>> a8f5a41106c0275f1944059736563fc2bdc80753
}
