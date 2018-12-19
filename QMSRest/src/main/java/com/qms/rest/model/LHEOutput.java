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
public class LHEOutput {
	//Member_ID	Enrollment_Gaps	Out_of_Pocket_Expenses	Utilizer_Category	Age	Amount_Spend	
	//ER	Reason_to_not_Enroll	Likelihood_to_Enroll
	
	private String memberId;
	private String memberName;
	private String enrollGaps;
	private String outOfPocketExpenses;
	private String utilizerCategory;
	private String age;
	private String amountSpend;
	private String er;
	private String reasonNotEnroll;
	private String likeliHoodEnroll;
	private String enrollmentBin;	

}
