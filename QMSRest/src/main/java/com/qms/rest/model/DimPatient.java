package com.qms.rest.model;

import java.util.List;
import java.util.Set;

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
public class DimPatient {

	private String patId;
	private String ptyId;
	private String emrPatId;
	private String ssn;
	private String mrn;
	private String firstName;
	private String middleName;
	private String lastName;
	private String city;
	private String addLine1;
	private String addLine2;
	private String state;
	private String county;
	private String country;
	private String race;
	private String zip;
	private String deathDate;
	private String birthDate;
	private String emailAddress;
	private String maritialStatus;
	private String language;
	private String gender;
	private String lngtd;
	private String lattd;	
	private String ethniCity;
	private String currFlag;
	private String createDate;
	private String updateDate;
	
	//for other fields - AggregateFactMember
	private Set<String> comorbidities;	
	private String comorbiditiesCount;
	private Set<String> careGaps;
	private String careGapsCount;
	private String ipVisitsCount;
	private String opVisitsCount;
	private String erVisitsCount;
	private String prescription;
	private String nextAppointmentDate;
	private String physicianName;
	private String department;
	private String procedureName1;
	private String procedureDateTime1;
	private String procedureName2;
	private String procedureDateTime2;	
	private String lastDateService;
	
	//for providerdetails
	private String providerFirstName;
	private String providerLastName;
	private String providerAddress1;
	private String providerAddress2;
	private String providerBillingTaxId;
	private String providerSpeciality;	
	
	//added fileds
	private String address;
	private String name;
	private String phone;
	private String age;
	private String primaryPayer;
	private String mraScore;
	private String risk;
	private String noShowLikelihood;
	
}
