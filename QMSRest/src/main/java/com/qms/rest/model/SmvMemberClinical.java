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
public class SmvMemberClinical {
	private String procedureName;
	private String drugCode;
	private String encCsnId;
	private String memberId;
	private String immunizationName;
	private String immunizationStatus;
	private String encounterDateSk;
}
