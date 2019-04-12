package com.itc.ncqa.Constants

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KpiConstants {





  /*Common Constants*/
  //val competingDiagnosisVal = "Competing Diagnosis"
  val nutritionCounselVal = "Nutrition Counseling"
  val physicalActCounselVal = "Physical Activity Counseling"
  val ncqaDataSource = "NCQA"
  val clientDataSource = ""
  val userNameVal = "ETL"
  val arrayOfColumn = List("member_id", "date_of_birth_sk", "gender", "lob",/*"location_sk",*/"product_plan_sk","member_plan_start_date_sk","member_plan_end_date_sk" /*,"primary_diagnosis", "procedure_code","start_date_sk", "PROCEDURE_CODE_MODIFIER1", "PROCEDURE_CODE_MODIFIER2", "PROCEDURE_HCPCS_CODE", "CPT_II", "CPT_II_MODIFIER", "DIAGNOSIS_CODE_2", "DIAGNOSIS_CODE_3", "DIAGNOSIS_CODE_4", "DIAGNOSIS_CODE_5", "DIAGNOSIS_CODE_6", "DIAGNOSIS_CODE_7", "DIAGNOSIS_CODE_8", "DIAGNOSIS_CODE_9", "DIAGNOSIS_CODE_10"*/)
  val arrayofColumn1 = List("member_id" , "state" , "city" , "member_plan_start_date_sk" , "member_plan_end_date_sk","lob_id")
  val arrayOfColumn2 = List("member_id", "patient_sk", "date_of_birth_sk", "gender", "lob","location_sk","product_plan_sk","member_plan_start_date_sk","member_plan_end_date_sk")
  val avoidCodeList = List("1P", "2P", "3P", "8P")
  var dbName = ""
  val yesVal =  "Y"
  val noVal  =  "N"
  val zeroVal = "0"
  val oneVal = "1"
  val actFlgVal = "A"
  val emptyStrVal = ""
  val maleVal = "M"
  val femaleVal = "F"
  val emptyList = List.empty[String]
  val boolTrueVal = true
  val boolFalseval = false
  val commercialLobName = "Commercial"
  val medicareLobName = "Medicare"
  val medicaidLobName = "Medicaid"
  val marketplaceLobName = "Marketplace"
  val mmdLobName = "Medicare-Medicaid Plans"
  val dateFormatString = "yyyy-MM-dd"


  val lobProductNameConVal = "Special Needs Plan-Institutionalized"



  /*Payer constant values*/
  val sn1PayerVal = "SN1"
  val sn2PayerVal = "SN2"
  val sn3PayerVal = "SN3"
  val mdePayerVal = "MDE"
  val mcrPayerVal = "MCR"
  val mcdPayerVal = "MCD"
  val mdPayerVal = "MD"
  val mliPayerVal = "MLI"
  val mrbPayerVal = "MRB"
  val mmpPayerVal = "MMP"

  /*function for setting the dbName with the value getting as argument */
  def setDbName(dbVal:String):String={
    dbName = dbVal
    dbName
  }

  /*claim status constants*/
  val paidVal = "1"
  val suspendedVal = "2"
  val pendingVal = "pending"
  val deniedVal = "denied"

  /*Continuous Enrollment Check format Constants*/
  val commondateformatName = "common_date_format"
  val ageformatName = "age_format"


  /*map key constants*/
  val ageStartKeyName = "ageStart"
  val ageEndKeyName = "ageEnd"
  val ageAnchorKeyName = "ageAnchor"
  val dateStartKeyName = "dateStart"
  val dateEndKeyName = "dateEnd"
  val dateAnchorKeyName = "dateAnchor"
  val lobNameKeyName = "lobName"
  val benefitKeyName = "benefit"

  /*age calculation constants*/
  val age1999Val = "19.99"
  val age0Val = "0"
  val age1Val = "1"
  val age2Val = "2"
  val age3Val = "3"
  val age4Val = "4"
  val age5Val = "5"
  val age6Val = "6"
  val age7Val = "7"
  val age9Val = "9"
  val age10Val = "10"
  val age11Val = "11"
  val age12Val = "12"
  val age13Val = "13"
  val age14Val = "14"
  val age15Val = "15"
  val age16Val = "16"
  val age17Val = "17"
  val age18Val = "18"
  val age19Val = "19"
  val age20Val = "20"
  val age21Val = "21"
  val age22Val = "22"
  val age24Val = "24"
  val age30Val = "30"
  val age40Val = "40"
  val age42Val = "42"
  val age44Val = "44"
  val age45Val = "45"
  val age51Val = "51"
  val age52Val = "52"
  val age64Val = "64"
  val age65Val = "65"
  val age66Val = "66"
  val age67Val = "67"
  val age74Val = "74"
  val age75Val = "75"
  val age80Val = "80"
  val age81Val = "81"
  val age85Val = "85"
  val age86Val = "86"
  val age120Val = "120"


  /*count constants*/
  val count0Val = 0
  val count1Val = 1
  val count2Val = 2
  val count4Val = 4
  val count3Val = 3


  /*days constant*/
  val days320Val = "320"
  val days321Val = "321"
  val days60 = 60
  val days730 = 730
  val days365 = 365
  val days30 = 30
  val days31 = 31
  val days42 = 42
  val days3 = 3
  val days0 = 0
  val days146 = 146
  val days180 = 180
  val days456 = 456
  val days45 = 45


  /*month constants*/
  val months24 = 24
  val months12 = 12
  val months156 = 156
  val months168 = 168
  val months144 = 144
  val months132 = 132
  val months120 = 120
  val months108 = 108
  val months216 = 216
  val months624 = 624
  val months240 = 240
  val months792 = 792
  val months972 = 972
  val months888 = 888
  val months900 = 900
  val months1032 = 1032

  /*measurement Year Constants*/
  val measurementYearLower = 0
  val measurementOneyearUpper = 365
  val measuremetTwoYearUpper = 730
  val measurementFourYearUpper = 1460
  val measurementNineYearUpper = 3287
  val measurementThreeYearUpper = 1096
  val measureemtnTenYearUpper = 3650
  val measurement0Val = 0
  val measurement1Val = 1
  val measurement2Val = 2
  val measurement3Val = 3
  val measurement4Val = 4
  val measurement5Val = 5
  val measurement9Val = 9
  val measurement10Val = 10




  /*Table Names*/
  val dimMemberTblName = "dim_member"
  val dimPatientTblName = "dim_patient"
  val dimDateTblName = "dim_date"
  val dimProviderTblName = "dim_provider"
  val dimLocationTblName = "dim_location"
  val dimProductTblName = "dim_product_plan"
  val dimQltyMsrTblName = "dim_quality_measure"
  val dimQltyPgmTblName = "dim_quality_program"
  val dimFacilityTblName = "dim_facility"
  val factClaimTblName = "fact_claims"
  val factencTblName = "fact_enc"
  val factencdxTblName = "fact_enc_dx"
  val factImmunTblName = "fact_immunization"
  val factpatmemTblName = "fact_pat_mem"
  val factMembershipTblName = "fact_membership"
  val factMonMembershipTblName = "fact_monthly_membership"
  val factRxClaimTblName = "fact_rx_claims"
  val factMemAttrTblName = "fact_mem_attribution"
  val factGapsInHedisTblName = "fact_hedis_gaps_in_care"
  val factHedisQmsTblName = "fact_hedis_qms"
  val refHedisTblName = "ref_hedis2019"
  val refLobTblName = "ref_lob"
  val refmedValueSetTblName = "ref_med_value_set"
  val view45Days = "gap_45_days_view"
  val view60Days = "gap_60_days_view"
  val outGapsInHedisTestTblName = "fact_hedis_gaps_in_care"
  val outFactHedisGapsInTblName = "fact_hedis_gaps_in_care"
  val outFactQmsTblName = "fact_hedis_qms"
  val visitTblName = "visits"
  val membershipTblName = "membership_enrollment"
  val generalmembershipTblName = "member"
  val providerTblName = "provider"
  val medmonmemTblName = "medicare_monthly_membership"
  val productPlanTblName = "product_plan"


  /*dataframe name constants*/
  val initjoinDfName = "initialjoin_df"
  val totalPopDfName = "totalPop_df"
  val eligibleDfName = "eligible_df"
  val mandatoryExclDfname = "mandatoryExcl_df"
  val optionalExclDfName = "optionalExcl_df"
  val numeratorDfName = "numerator_df"
  val numerator2DfName = "numerator2_df"
  val numerator3DfName = "numerator3_df"
  val numerator4DfName = "numerator4_df"
  val numerator5DfName = "numerator5_df"
  val numerator6DfName = "numerator6_df"
  val numerator7DfName = "numerator7_df"


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
  val imaMeasureTitle = "Immunizations for Adolescents"
  val wccMeasureTitle = "Weight Assessment and Counseling for Nutrition and Physical Activity for Children/Adolescent"
  val aapMeasureTitle = "Adult Access to Preventive/Ambulatory Health Services (AAP)"
  val aisMeasureTitle = "Adult Immunization Status (AIS)"
  val capMeasureTitle = "Children and Adolescents Access to Primary Care Practitioners (CAP)"
  val cbpMeasureTitle = "Controlling High Blood Pressure (CBP)"
  val ccsMeasureTitle = "Cervical Cancer Screening"
  val cwpMeasureTitle = "Appropriate Testing for Children with Pharyngitis (CWP)"
  val uriMeasureTitle = "Appropriate Treatment for Children with Upper Respiratory Infection (URI)"
  val sprMeasureTitle = "Use of Spirometry Testing in the Assessment and Diagnosis of COPD (SPR)"
  val smdMeasureTitle = "Diabetes Monitoring for People with Diabetes and Schizophrenia (SMD)"
  val ssdMeasureTitle = "Diabetes Screening for People with Schizophrenia or Bipolar Disorder Who Are Using Antipsychotic Medications (SSD)"
  val ncsMeasureTitle = "NonReended Cervical Cancer Screening in Adolescent Females (NCS)"

  /*program name constants*/
  val hedisPgmname = "HEDIS"




  /*columnname constants*/
  val memberskColName = "member_sk"
  val lobIdColName = "lob_id"
  val lobNameColName = "lob_name"
  val dobskColame = "date_of_birth_sk"
  val dateSkColName = "date_sk"
  val calenderDateColName = "calendar_date"
  val qualityMsrSkColName = "quality_measure_sk"
  val memPlanStartDateSkColName = "member_plan_start_date_sk"
  val memPlanEndDateSkColName = "member_plan_end_date_sk"
  val productplanSkColName = "product_plan_sk"
  val startDateColName = "start_date"
  val maxserviceDateColName = "max_service_date"
  val immStartDateSkColname = "immune_date_sk"
  val immuneDateColName = "immune_date"
  val immuneDate2ColName = "immune_date2"
  val expDateSkColName = "exp_date_sk"
  val expDateColName = "exp_date"
  val patientSkColname = "patient_sk"
  val patientidColname = "pat_id"
  val rxStartDateColName = "rx_start_date"
  val rxServiceDateColName = "rx_service_date"
  val endDateColName = "end_date"
  val locationSkColName = "location_sk"
  val facilitySkColName = "facility_sk"
  val providerSkColName = "provider_sk"
  val pcpColName = "pcp"
  val obgynColName = "obgyn"
  val eyeCareProvColName = "eye_care_provider"
  val nephrologistColName = "nephrologist"
  val measureIdColName = "measureid"
  val measure_idColName = "measure_id"
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
  val mrbpreadingColName = "most_recent_bpreading"
  val contenrollLowCoName = "continuous_Enroll_LowerDate"
  val contenrollUppCoName = "continuous_Enroll_UpperDate"
  val anchorDateColName = "anchor_date"
  val month15ColName = "month_15"
  val secondDobColName = "second_dob"
  val firstDobColName = "first_dob"
  val thirteenDobColName = "thirteen_dob"
  val twelveDobColName = "twelve_dob"
  val elevenDobColName = "eleven_dob"
  val tenDobColName = "ten_dob"
  val nineDobColName = "nine_dob"
  val provprespriColName = "provider_prescribing_privileges"
  val clinphaColName = "clinical_pharmacist"
  val rankColName = "rank"
  val minMemStDateColName = "min_mem_start_date"
  val maxMemEndDateColName = "max_mem_end_date"
  val datediffColName = "date_diff"
  val overlapFlagColName = "overlap_flag"
  val coverageDaysColName = "coverage_days"
  val maxDateDiffColName = "max_date_diff"
  val leadColName = "lead_col"
  val anchorflagColName = "anchor_flag"
  val contEdFlagColName = "cont_enroll_flag"
  val secondContFlag = "second_cont_flag"
  val serviceTempColName = "service_temp"
  val admitTempColName = "admit_temp"
  val dischargeTempColName = "discharge_temp"
  val sumBenefitColName = "sum_benefit"
  val countBenefitColName = "count_benefit"
  val secondDiagColName = "second_diagnosis"
  val medicatiolListColName = "medication_list"
  val lisPremiumSubColName = "lis_premium_subsidy"
  val orgreasentcodeColName = "ori_reason_for_entitle_code"
  val measureColName = "measure"
  val reqCovDaysColName = "requiredCoverageDays"


  /*member table column name constants*/
  val memberidColName = "member_id"
  val dobColName = "dob"
  val genderColName = "gender"
  val stateColName = "state"
  val dateofbirthColName = "date_of_birth"


  /*membership col names*/
  val payerColName = "payer"
  val productColName = "product"
  val lobColName = "lob"
  val lobproductColName = "lob_product"
  val plannameColName = "plan_name"
  val primaryPlanFlagColName = "primary_plan_flag"
  val memStartDateColName = "member_plan_start_date"
  val memEndDateColName = "member_plan_end_date"
  val benefitdentalColName = "benefit_dental"
  val benefitdrugColName = "benefit_drug"
  val benefitmhiColName = "benefit_mental_health_inpatient"
  val benefitmhioColName = "benefit_mental_health_intensive_outpatient"
  val benefitmhoedColName = "benefit_mental_health_outpatient_ed"
  val benefitchmdepinpatColName = "benefit_chemdep_inpatient"
  val benefitcheintoutpatColName = "benefit_chemdep_intensive_outpatient"
  val benefitcheoutpatedColName = "benefit_chemdep_outpatient_ed"
  val benefitMedicalColname = "benefit_medical"
  val healthpefColName = "health_plan_employee_flag"
  val considerationsColName = "considerations"




  /*visit table column name cnstants*/
  val claimsidColName = "claims_id"
  val claimlineidColName = "claim_line_id"
  val patientencscnidColName = "pat_enc_csn_id"
  val parentenccsnidColName = "parent_enc_csn_id"
  val serviceDateColName = "service_date"
  val servicestdateColName = "service_start_date"
  val serviceeddateColname = "service_end_date"
  val admitDateColName = "admit_date"
  val dischargeDateColName = "discharge_date"
  val covereddaysColName = "covered_days"
  val claimstatusColName = "claim_status"
  val proceedureCodeColName = "procedure_code"
  val primaryDiagnosisColname = "primary_diagnosis"
  val proccodemod1ColName = "procedure_code_modifier1"
  val proccodemod2ColName = "procedure_code_modifier2"
  val proccode2mod1ColName = "procedure_code_2_modifier1"
  val proccode2mod2ColName = "procedure_code_2_modifier2"
  val proccodeColName = "procedure_code"
  val proccode2ColName = "procedure_code_2"
  val prochcpscodeColName = "procedure_hcpcs_code"
  val prochcpsmod1codeColName = "procedure_hcpcs_modifier1_code"
  val prochcpsmod2codeColName = "procedure_hcpcs_modifier2_code"
  val prochcpsmod3codeColName = "procedure_hcpcs_modifier3_code"
  val prochcpsmod4codeColName = "procedure_hcpcs_modifier4_code"
  val prochcpsmod5codeColName = "procedure_hcpcs_modifier5_code"
  val diagcode2ColName = "diagnosis_code_2"
  val diagcode3ColName = "diagnosis_code_3"
  val diagcode4ColName = "diagnosis_code_4"
  val diagcode5ColName = "diagnosis_code_5"
  val diagcode6ColName = "diagnosis_code_6"
  val diagcode7ColName = "diagnosis_code_7"
  val diagcode8ColName = "diagnosis_code_8"
  val diagcode9ColName = "diagnosis_code_9"
  val diagcode10ColName = "diagnosis_code_10"
  val priicdprocColName = "principal_icd_procedure"
  val icdproc2ColName = "icd_procedure_2"
  val icdproc3ColName = "icd_procedure_3"
  val icdproc4ColName = "icd_procedure_4"
  val icdproc5ColName = "icd_procedure_5"
  val icdproc6ColName = "icd_procedure_6"
  val icdflagColName = "icd_flag"
  val loinccodeColName = "loinc_code"
  val labvalueColName = "lab_value"
  val obscodetextColName = "obs_code_text"
  val billtypecodeColName = "bill_type_code"
  val poscodeColName = "pos_code"
  val drgcodeColName = "drg_code"
  val claimtypeColName = "claim_type"
  val revenuecodeColName = "revenue_code"
  val admissioncodeColName = "admission_code"
  val admissiontypeColName = "admission_type"
  val dischargestatuscodeColName = "discharge_status_code"
  val provideridColName = "provider_id"
  val ispcpColName = "is_pcp"
  val isobgynColName = "is_obgyn"
  val ismhproviderColName = "is_mh_provider"
  val iseyecareproviderColName = "is_eye_care_provider"
  val isdentistColName = "is_dentist"
  val isnephrologistColName = "is_nephrologist"
  val isanesthesiologistColName = "is_anesthesiologist"
  val isnprproviderColName = "is_npr_provider"
  val ispasproviderColName = "is_pas_provider"
  val provprescprivColName = "provider_prescribing_privileges"
  val clinicalpharmacistColName = "clinical_pharmacist"
  val isprovhospitalColName = "is_provider_hospital"
  val isprovidersnfColName = "is_provider_snf"
  val issurgeonColName = "is_surgeon"
  val isregnurseColName = "is_registered_nurse"
  val locofcareColName = "location_of_care"
  val amountpaidColName = "amount_paid"
  val rxdayssuppliedColName = "rx_days_supplied"
  val ndcNmberColName = "ndc_number"
  val ndcCodeColName = "ndc_code"
  val rxmetquanColName = "rx_metric_quantity"
  val rxdispquanColName = "rx_dispensed_quantity"
  val providernpiColName = "provider_npi"
  val pharmacynpiColName = "pharmacy_npi"
  val obstestColName = "observation_test"
  val test_codeColName = "test_code"
  val codetextColName = "code_text"
  val snomedColName = "snomed"
  val obsvalueColName = "obs_value"
  val obsunitsColName = "obs_units"
  val obsstatusColName = "obs_status"
  val obsrestypeColName = "obs_result_type"
  val obsresvalflagColName = "obs_result_value_flag"
  val obstypeflagColName = "obs_type_flag"
  val medorderdateColName = "med_order_date"
  val medstartdateColName = "med_start_date"
  val medcodeColName = "med_code"
  val medcodeflagColName = "med_code_flag"
  val medcodetextColName = "med_code_text"
  val medfrequencyColName = "med_frequency"
  val meddispdateColName = "med_disp_date"
  val medenddateColName = "med_end_date"
  val medactiveflagColName = "med_active_flag"
  val immunizationyearColName = "immunization_year"
  val apptstatusColName = "appt_status"
  val lobProductColName = "lob_product"
  val planColName = "plan"
  val supplflagColName = "supplement_flag"
  val labCodeColName = "lab_code"
  val dataSourceColName = "data_source"
  val secondServiceDateColName = "second_service_date"


  /*medicare_monthly_membership table column names*/
  val hospiceFlagColName = "hospice"
  val ltiFlagColName = "lti_flag"
  val runDateColName = "run_date"
  val orecCodeColName = "ori_reason_for_entitle_code"
  val lisPreSubsColName = "lis_premium_subsidy"


  /*Common Schema Constants*/
  val memberIdSchema = StructType(Array(StructField(memberidColName, StringType)))








  /*audit column names*/
  val ingestiondateColName = "ingestion_date"
  val activeflasgColname = "active_flag"
  val latestflagColName = "latest_flag"


  /*fact_claims column name constants*/

  val startDateSkColName = "start_date_sk"
  val serviceDateSkColName = "service_date_sk"
  val admitDateSkColName = "admit_date_sk"
  val dischargeDateSkColName = "discharge_date_sk"


  /*ref_hedis2019 column constants*/
  val measureidColname = "measureid"
  val codeColName = "code"
  val valuesetColName = "valueset"
  val codesystemColname = "codesystem"



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




  /*NCQA outputformat Column Names*/
  val ncqaOutmemberIdCol = "MemID"
  val ncqaOutMeasureCol = "Meas"
  val ncqaOutPayerCol = "Payer"
  val ncqaOutEpopCol = "Epop"
  val ncqaOutExclCol = "Excl"
  val ncqaOutNumCol = "Num"
  val ncqaOutRexclCol ="RExcl"
  val ncqaOutIndCol = "Ind"
  val ncqaOutHmoCol = "HMO"
  val ncqaPosOutCol = "POS"
  val ncqaPpoOutCol = "PPO"
  val ncqaCepOutCol = "CEP"
  val ncqaMcrOutCol = "MCR"
  val ncqaMcOutCol = "MC"
  val ncqaMcsOutCol = "MCS"
  val ncqaMpOutCol = "MP"
  val ncqaMcdOutCol = "MCD"


  /*order of columns in ncqa Ouput format*/
  val outncqaFormattedList = List(ncqaOutmemberIdCol,ncqaOutMeasureCol,ncqaOutPayerCol,ncqaOutEpopCol,ncqaOutExclCol,ncqaOutNumCol,ncqaOutRexclCol,ncqaOutIndCol)


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
  val adv1MeasureId = "ADV1"
  val adv2MeasureId = "ADV2"
  val adv3MeasureId = "ADV3"
  val adv4MeasureId = "ADV4"
  val adv5MeasureId = "ADV5"
  val adv6MeasureId = "ADV6"
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
  val lscMeasureId  = "LSC"
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
  val cisMeasureId = "CIS"
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
  val colMeasureId = "COL"
  val bcsMeasureId = "BCS"
  val imaMeasureId = "IMA"
  val imamenMeasureId = "IMAMEN"
  val imatdMeasureId = "IMATD"
  val imahpvMeasureId = "IMAHPV"
  val imacmb1MeasureId = "IMACMB1"
  val imacmb2MeasureId = "IMACMB2"
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
  val aapMeasureId = "AAP"
  val aap1MeasureId = "AAP1"
  val aap2MeasureId = "AAP2"
  val aap3MeasureId = "AAP3"
  val capMeasureId = "CAP"
  val cap1MeasureId = "CAP1"
  val cap2MeasureId = "CAP2"
  val cap3MeasureId = "CAP3"
  val cap4MeasureId = "CAP4"
  val cbpMeasureId = "CBP"
  val ncsMeasureId = "NCS"
  val cwpMeasureId = "CWP"
  val uriMeasureId = "URI"
  val sprMeasureId = "SPR"
  val smdMeasureId = "SMD"
  val ssdMeasureId = "SSD"
  val coaMeasureId = "COA"
  val coa1MeasureId = "COA1"
  val coa2MeasureId = "COA2"
  val coa3MeasureId = "COA3"
  val coa4MeasureId = "COA4"
  val ggMeasureId = "GG"




  /*Valueset Constants*/
  val outPatientVal = "Outpatient"
  val pregnancyVal = "Pregnancy"
  val bmiVal = "BMI"
  val bmiPercentileVal = "BMI Percentile"
  val sexualActivityVal = "Sexual Activity"
  val diagnosticRadVal = "Diagnostic Radiology"
  val pregnancyTestVal = "Pregnancy Tests"
  val pregnancyExcltestVal = "Pregnancy Test Exclusion"
  val retinoidMedicationval = "Retinoid Medications"
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
  val bilateralModVal = "Bilateral Modifier"
  val unilateralEyeEnuLeftVal = "Unilateral Eye Enucleation Left"
  val unilateralEyeEnuRightVal = "Unilateral Eye Enucleation Right"
  val diabetesExclusionVal = "Diabetes Exclusions"
  val urineProteinTestVal = "Urine Protein Tests"
  val nephropathyTreatmentVal= "Nephropathy Treatment"
  val kidneyTransplantVal = "Kidney Transplant"
  val systolicLt140Val = "Systolic Less Than 140"
  val systolicGtOrEq140Val = "Systolic Greater Than/Equal To 140"
  val diastolicLt80Val = "Diastolic Less Than 80"
  val diastolicBtwn8090Val = "Diastolic 80-89"
  val diastolicGt90Val = "Diastolic Greater Than/Equal To 90"
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
  val nonacuteInPatStayVal = "Nonacute Inpatient Stay"
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
  val contraceptiveMedicationVal = "Contraceptive Medications"
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
  val unilateralMastectomyLeftVal = "Unilateral Mastectomy Left"
  val unilateralMastectomyRightVal = "Unilateral Mastectomy Right"
  val bilateralModifierVal = "Bilateral Modifier"
  val historyBilateralMastectomyVal = "History of Bilateral Mastectomy"
  val leftModifierVal = "Left Modifier"
  val rightModifierVal = "Right Modifier"
  val absOfLeftBreastVal = "Absence of Left Breast"
  val absOfRightBreastVal = "Absence of Right Breast"
  val ardvVal = "Anaphylactic Reaction Due To Vaccination"
  val encephalopathyVal = "Encephalopathy Due To Vaccination"
  val tdVaccineVal = "Td Vaccine"
  val tdapVaccineVal = "Tdap Vaccine"
  val boneMarowTransVal = "Bone Marrow Transplant"
  val chemoTherappyVal = "Chemotherapy"
  val immunoCompromisingVal = "Immunocompromising Conditions"
  val cochlearImplantVal = "Cochlear Implant"
  val afaVal = "Anatomic or Functional Asplenia"
  val scaHbsdVal = "Sickle Cell Anemia and HB-S Disease"
  val cflVal = "Cerebrospinal Fluid Leak"
  val hospiceVal = "Hospice"
  val influenzaVaccineVal = "Influenza Vaccine Administered"
  val herpesZosterLiveVaccineVal = "Herpes Zoster Live Vaccine"
  val herpesZosterRecomVaccineVal = "Herpes Zoster Recombinant Vaccine"
  val pneuConjuVaccine13Val = "Pneumococcal Conjugate Vaccine 13"
  val pneuPolyVaccine23Val = "Pneumococcal Polysaccharide Vaccine 23"
  val ambulatoryVisitVal = "Ambulatory Visits"
  val mammographyVal = "Mammography"
  val aceInhArbMedVal = "ACE Inhibitor/ARB Medications"
  val otherAmbulatoryVal = "Other Ambulatory Visits"
  val essentialHyptenVal = "Essential Hypertension"
  val outpatwoUbrevVal = "Outpatient Without UBREV"
  val remotebpmVal = "Remote Blood Pressure Monitoring"
  val absOfCervixVal = "Absence of Cervix"
  val cervicalCytologyVal = "Cervical Cytology"
  val hpvTestVal = "HPV Tests"
  val cervicalCancerVal = "Cervical Cancer"
  val hivVal = "HIV"
  val hivType2Val = "HIV Type 2"
  val disordersoftheImmuneSystemVal = "Disorders of the Immune System"
  val hpvTestsVal = "HPV Tests"
  val pharyngitisVal = "Pharyngitis"
  val cwpAntibioticMedicationListsVal = "CWP Antibiotic Medications"
  val groupAStrepTestsVal = "Group A Strep Tests"
  val uriVal = "URI"
  val copdVal = "COPD"
  val emphysemaVal = "Emphysema"
  val chronicBronchitisVal = "Chronic Bronchitis"
  val nonAcuteInPatientStayVal = "Nonacute Inpatient Stay"
  val competingDiagnosisVal = "Competing Diagnosis"
  val glucoseTestsValueSet = "Glucose Tests"
  val otherBipolarDisorder = "Other Bipolar Disorder"
  val visitSettingUnspecifiedVal = "Visit Setting Unspecified"
  val smcMeasureTitle = "Cardiovascular Monitoring for People with Cardiovascular Disease and Schizophrenia (SMC)"
  val smcMeasureId = "SMC"
  val bHStandAloneAcuteInpatientVal = "BH Stand Alone Acute Inpatient"
  val schizophreniaVal = "Schizophrenia"
  val bipolarDisorderVal = "Bipolar Disorder"
  val acuteInpatientPosVal = "Acute Inpatient POS"
  val outpatientPosVal = "Outpatient POS"
  val bhOutpatientVal = "BH Outpatient"
  val partialHospitalizationPosVal = "Partial Hospitalization POS"
  val partialHospitalizationIntensiveOutpatientVal = "Partial Hospitalization/Intensive Outpatient"
  val communityMentalHealthCenterPosVal = "Community Mental Health Center POS"
  val electroconvulsiveTherapyVal = "Electroconvulsive Therapy"
  val edPosVal = "ED POS"
  val bhStandAloneNonacuteInpatientVal = "BH Stand Alone Nonacute Inpatient"
  val nonacuteInpatientPosVal = "Nonacute Inpatient POS"
  val telephonePosVal = "Telehealth POS"
  val amiVal ="AMI"
  val ldlcTestsVal = "LDL-C Tests"
  val spirometryVal = "Spirometry"
  val cptCatIIVal = "CPT-CAT-II"
  val longActingInjVal = "Long-Acting Injections"
  val antipsychoticMedVal = "SSD Antipsychotic Medications"
  val anaReactVacVal = "Anaphylactic Reaction Due To Vaccination"
  val vacCauseAdvEffVal = "Vaccine Causing Adverse Effect"
  val mallNeoLymTisVal = "Malignant Neoplasm of Lymphatic Tissue"
  val severeCombImmunVal = "Severe Combined Immunodeficiency"
  val intussusceptionVal = "Intussusception"
  val dtapVacAdmVal = "DTaP Vaccine Administered"
  val ipvVaccineVal = "Inactivated Polio Vaccine (IPV) Administered"
  val mmrVacVal = "Measles, Mumps and Rubella (MMR) Vaccine Administered"
  val mrVacVal = "Measles/Rubella Vaccine Administered"
  val mumpsVal = "Mumps"
  val mumpsVacVal = "Mumps Vaccine Administered"
  val measlesVacVal = "Measles Vaccine Administered"
  val measelesVal = "Measles"
  val rubellaVacVal = "Rubella Vaccine Administered"
  val rubellaVal = "Rubella"
  val hibVacVal = "Haemophilus Influenzae Type B (HiB) Vaccine Administered"
  val pneConjVacVal = "Pneumococcal Conjugate Vaccine Administered"
  val influenzaVacVal = "Influenza Vaccine Administered"
  val rotavirusVac2Val = "Rotavirus Vaccine (2 Dose Schedule) Administered"
  val rotavirusVac3Val = "Rotavirus Vaccine (3 Dose Schedule) Administered"
  val hepatitisAVacVal = "Hepatitis A Vaccine Administered"
  val hepatitisAVal= "Hepatitis A"
  val ardtsVal = "Anaphylactic Reaction Due To Serum"
  val vaccineAdverseVal = "Vaccine Causing Adverse Effect"
  val meningococcalVal = "Meningococcal Vaccine Administered"
  val tdapVacceVal = "Tdap Vaccine Administered"
  val hpvVal = "HPV Vaccine Administered"
  val vzvVacVal = "Varicella Zoster (VZV) Vaccine Administered"
  val vzvVal = "Varicella Zoster"
  val hepatitisBVacVal = "Hepatitis B Vaccine Administered"
  val hepatitisBVal = "Hepatitis B"
  val advanceCarePlanning = "Advance Care Planning"
  val medicationReviewVal = "Medication Review"
  val medicationListVal = "Medication List"
  val transitionalCareMgtSerVal = "Transitional Care Management Services"
  val functionalStatusVal = "Functional Status Assessment"
  val painAssessmentVal = "Pain Assessment"
  val independentLabVal = "Independent Laboratory"

  val lionicValList = List(leadTestVal, pregnancyExcltestVal)






  /*Codesystem constants*/
  val icdCodeVal = "ICD%"
  val icd9cmCodeVal = "ICD9CM"
  val icd10cmCodeVal = "ICD10CM"
  val icd9pcsCodeVal = "ICD9PCS"
  val icd10pcsCodeVal = "ICD10PCS"
  val cptCodeVal = "CPT"
  val cptcat2CodeVal = "CPT-CAT-II"
  val hcpsCodeVal = "HCPCS"
  val ubrevCodeVal = "UBREV"
  val ubtobCodeVal = "UBTOB"
  val loincCodeVal = "LOINC"
  val modifierCodeVal = "Modifier"
  val posCodeVal = "POS"
  val cvxCodeVal = "CVX"
  val hl7CodeVal = "HL7"
  val rxnormCodeVal = "RXNORM"
  val snomedctCodeVal = "SNOMED CT US Edition"
  val bilateralCodeVal = "bilateral (2-view study of each breast)"
  val g204CodeVal = "including computer-aided detection (cad) when performed; bilateral (G0204)"
  val g206CodeVal = "including computer-aided detection (cad) when performed; unilateral (G0206)"
  val allInclusiveCodeVal = "all-inclusive (T1015)"
  val ppsCodeVal = "includes a personalized prevention plan of service (pps)"
  val initialCodeVal = "initial visit (G0438)"
  val g0402CodeVal = "services limited to new beneficiary during the first 12 months of medicare enrollment (G0402)"
  val unspecifiedCodeVal = "unspecified"

  val codeSystemList = List(icd9cmCodeVal,icd10cmCodeVal,icd9pcsCodeVal,icd10pcsCodeVal,cptCodeVal,cptcat2CodeVal,hcpsCodeVal,ubrevCodeVal,
                            ubtobCodeVal, loincCodeVal, modifierCodeVal, posCodeVal, cvxCodeVal,rxnormCodeVal,snomedctCodeVal)




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

  val cbpDinoExclValueSet = List("Frailty")

  val cbpDinoExclCodeSystem = List("CPT", "HCPCS")

  val cbpCommonDinominatorValueSet = List("Essential Hypertension")

  val cbpDinominator1ValueSet = List("Outpatient","Telehealth Modifier")

  val cbpDinominator2ValueSet = List("Telephone Visits")

  val cbpDinominator3ValueSet = List("Online Assessments")

  val cbpDinominator4ValueSet = List("Online Assessments")



  val cbpDinominator1CodeSystem = List("CPT","HCPCS","Modifier")

  val cbpDinominator2CodeSystem = List("CPT")

  val cbpDinominator3CodeSystem = List("CPT")

  val cbpDinominatorExcl2aCodeSystem = List("CPT","HCPCS","UBREV")

  val cbpDinominatorExcl2aValueSet = List("Outpatient","Observation","ED","Nonacute Inpatient")

  val cbpDinominatorICDExcl2ValueSet = List("Advanced Illness")

  val cbpDinominatorExcl3aCodeSystem = List("CPT","UBREV")

  val cbpDinominatorExcl3aValueSet = List("Acute Inpatient")

  val cbpNumerator1CodeSystem = List("CPT","HCPCS","UBREV")

  val cbpNumerator1ValueSet = List("Outpatient Without UBREV","Nonacute Inpatient","Remote Blood Pressure Monitoring")

  val cbpNumeratorSystolicCodeSystem = List("CPT-CAT-II")

  val cbpNumeratorSystolicValueSet = List("Systolic Less Than 140")

  val cbpNumeratorDiastolicCodeSystem = List("CPT-CAT-II")

  val cbpNumeratorDiastolicValueSet = List("Diastolic Less Than 80","Diastolic 80–89")

  val cbpOptionalExclusion1CodeSystem = List("CPT","HCPCS","POS","UBREV","UBTOB")

  val cbpOptionalExclusion1ValueSet = List("ESRD","ESRD Obsolete","Kidney Transplant")

  val cbpOptionalExclusionICDValueSet = List("ESRD","Kidney Transplant")

  val cbpPregnancyExclValueSet = List("Pregnancy")

  val cbpOptionalExclusion2CodeSystem = List("Inpatient Stay","Nonacute Inpatient Stay")

  val cbpOptionalExclusion2ValueSet = List("UBREV","UBTOB")


  val ccsMeasureId = "CCS"

  val ccsNumeratorStep1ValueSet = List("Cervical Cytology")

  val ccsNumeratorStep1CodeSystem = List("CPT","HCPCS","LOINC","UBREV")

  val ccsDinomenatorExclValueSet = List("Absence of Cervix")

  val ccsDinomenatorExclCodeSystem = List("CPT")

  val ccsNumeratorStep2ValueSet = List("HPV Tests")

  val ccsNumeratorStep2CodeSystem = List("CPT","HCPCS","LOINC","UBREV","UBTOB")


  val coaMeasureTitle = "Care of Older Adults (COA)"



  val coaAdvanceCareValueSet = List("Advance Care Planning")

  val coaAdvanceCareCodeSystem = List("CPT","CPT-CAT-II","HCPCS")

  val coaMedicationReviewCodeSystem = List("CPT","CPT-CAT-II")

  val coaMedicationReviewValueSet = List("Medication Review")

  val coaMedicationListCodeSystem = List("CPT-CAT-II","HCPCS")

  val coaMedicationListValueSet = List("Medication List")

  val coaTransitionalCareValueSet = List("Transitional Care Management Services")

  val coaTransitionalCareCodeSystem = List("CPT")

  val coaNumeratorExcludeValueSet = List("Acute Inpatient","Acute Inpatient POS")

  val coaNumeratorExcludeCodeSystem = List("CPT","POS","UBREV")

  val coaFunctionalStatusValueSet = List("Functional Status Assessment")

  val coaFunctionalStatusCodeSystem = List("CPT","CPT-CAT-II","HCPCS")

  val coaPainAssessmentValueSet = List("Pain Assessment")

  val coaPainAssessmentCodeSystem = List("CPT-CAT-II")






  val ssdLongActingInjectionsValueSet = List("Long-Acting Injections")
  val ssdAntipsychoticMedicationListVal = List("Antipsychotic Medications")









}
