package com.qms.rest.model;

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
public class LHCMember {
	private String memberID;	
	private String memberName;
	private String age;	
	private String gender;
	private String persona;
	private String participationQuotient;
	private String comorbidityCount;	
	private String frequencyExercise;	
	private String motivations;	
	private String enrollmentGaps;	
	private String amountSpend;	
	private String likelihoodChurn;
	private String churn;	
}
