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
public class SMVMemberPayerClustering {

	private String memberId;
	private String lob;
	private String code;
	private String planName;
	private String planCategory;
	private String memberPlanStartDateSk;
	private String memberPlanEndDateSk;
	private String noOfPendingClaimsYtd;
	private String noOfDeniedClaimsYtd;
	private String amountSpentYtd;
	private String personaName;
	private String preferredGoal;
	private String preferredReward;
	private String channel;
	private String likelihoodEnrollment;
	private String reasonToNotEnroll;
	
	private String physicalActivityFrequency;
	private String physicalActivityDate;
	private String calorieIntakeGoal;
	private String calorieIntakeFrequency;
	private String calorieIntakeDate;
	private String careGap;
	private String careGapDate; 
	private String category;
	private String goal;
	private String frequency;
	private String goalDate;
	private String reward;

}
