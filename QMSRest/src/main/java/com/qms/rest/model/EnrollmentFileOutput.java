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
public class EnrollmentFileOutput {
	
	private String memberId;
	private String memberName;
	private String channel;
	private String reward1;
	private String reward2;
	private String reward3;
	private String likelihoodEnrollment;
	private String reasonNotEnroll;
	private String age;
	private String gender;
	private String maritalStatus;
	private String amountSpend;
	private String utilizerCategory;
	private String comorbidityCount;
	private String enrollmentGaps;
	private String daysPendingTermination;
	private String crmFlag;
	private String verifyFlag;
	private String remarks;
	
}
