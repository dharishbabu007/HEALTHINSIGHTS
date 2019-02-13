package com.qms.rest.model;

import java.util.ArrayList;
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
public class Pat {
	
	private String mitId;
	private String measureSk;
	private String patientId;
	private String compliantFlag;
	private String lobId;
	private String mrn;
	private String appointmentDate;
	private String providerId;
	private String gender;
	private String dob;
	private String memberStatus;
	private String valueSet;
	private String codeType;
	private String codes;
	private String reason;
	private List<String> uploadFilesList = new ArrayList<>();
	
}