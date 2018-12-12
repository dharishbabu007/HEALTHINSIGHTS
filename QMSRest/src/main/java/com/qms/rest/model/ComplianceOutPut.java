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
public class ComplianceOutPut {
		
	private String patId;
	private String patName;
	private String age;
	private String gender;
	private String race;
	private String ethnicity;
	private String martialStatus;
	private String haveHighSchoolDegreeYarn;
	private String disabolityYorn;
	private String distenceFormNear;
	private String state;
	private String zipCode;
	private String country;
	private String pcpAssignYorn;
	private String empYorn;
	private String insuYorn;
	private String noOfdepts;
	private String noOfMissApp;
	private String noOfComplMeas;
	private String historyOfNonCom;
	private String hypertension;
	private String diabets;
	private String smokeYarn;
	private String alcoholYarn;
	private String mentalhealYarn;
	private String noOfIpVisit;
	private String noOfOpVisit;
	private String noOfErVisit;
	private String daySpending;
	private String planCoverRatio;
	private String compiancePotential;
	private String predictPotential;
	
}
