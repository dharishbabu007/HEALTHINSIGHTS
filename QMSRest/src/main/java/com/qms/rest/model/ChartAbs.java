package com.qms.rest.model;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ChartAbs {
	private int memberId;
	private int visit;
	private int visitLineId;
	private String firstName;
	private String lastName;
	private String dob;
	private String gender;
	private String provider;
	private String encounterStartDate;
	private String encounterEndDate;
	private List<String> encounterCode;
	private List<String> encounterValueset;
	private String allergiesDrug;
	private String allergiesDrugCode;
	private String allergiesReaction;
	private String allergiesStartDate;
	private String allergiesEndDate;
	private String allergiesNotes;
	private String allergiesOthers;
	private String allergiesOthersCode;
	private String allergiesOthersReaction;
	private String allergiesOthersStartDate;
	private String allergiesOthersEndDate;
	private String allergies_others_notes;
	private String medicationsNdcDrugCodeName;
	private String medicationsDrugCode;
	private String medicationsDaysSupplied;
	private String medicationsQuantity;
	private String medicationsNotes;
	private String medicationsPrescriptionDate;
	private String medicationsStartDate;
	private String medicationsDispensedDate;
	private String medicationsEndDate;
	private String medicationsProviderNpi;
	private String medicationsPharmacyNpi;
	private String immunizationsName;
	private String immunizationsCode;
	private String immunizationsStartDate;
	private String immunizationsEndDate;
	private String immunizationsNotes;
	private String disease;
	private String diseaseCode;
	private String diseaseDiagnosisDate;
	private String diseaseEndDate;
	private String diseaseNotes;
	private String sdohCategory;
	private String sdohCategoryCode;
	private String sdohProblem;
	private String sdohProblemCode;
	private String sdohNotes;
	private String substanceUse;
	private String substanceUseCode;
	private String substanceFrequency;
	private String substanceNotes;
	private String familyHistoryRelation;
	private String familyHistoryStatus;
	private String familyHistoryDisease;
	private String familyHistoryDiseaseCode;
	private String familyHistoryNotes;
	private String chartType;
}
