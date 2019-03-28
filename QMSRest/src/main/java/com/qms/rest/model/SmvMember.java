package com.qms.rest.model;

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
public class SmvMember {

	private String memberId;
	private String numberOfPendingClaims;
	private String pendingClaimsDate ;
	
	private String numberOfDeniedClaims;
	private String deniedClaimsDate;
	
	private String totalAmountSpend;
	private String amountSpendDate;
	
	private String enrollmentPrimaryPayer;
	private String enrollmentLob;
	private String enrollmentProduct;
	private String enrollmentPlan;
	private String enrollmentPlanType;
	private String enrollmentStartDate;
	private String enrollmentEndDate;
	
	private String engagementPersonaName;
	private String engagementLikelihoodEnrollment;
	private String engagementReasonToNotEnroll;
	private String engagementChannel;
	private String engagementLikelihoodOfEnrollment;
	private String engagementLikelihoodToRecommend;
	
	private String activeImmunizationsDate;
	private String activeImmunizations;
	
	private String pendingImmunizationsDate;
	private String PendingImmunizations;
	private String PendingImmunizationsStatus;
	
	private String lastPrescribedProceduresDate;
	private String lastPrescribedProcedures;
	
	private String lastPrescribedMedicationsDate;
	private String lastPrescribedMedications;
	
	private String ipVisits="0";
	private String opVisits="0";
	private String erVisits="0";
	
	private String lastDateOfService;
	private String risk;
	private Set<RewardSet> rewardSetList;
	
	private Set<String> comorbidities;	
	private String comorbiditiesCount;
	
	private Set<String[]> careGaps;
	private String careGapsCount;	
}
