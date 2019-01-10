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
public class PersonaDefine {
	private int clusterId;
	private String personaName;
	private String demographics;
	private String motivations;
	private String goals;
	private String barriers;
	private String socialMedia;
	private String healthStatus;	
	private String demoAgeGroup;
	private String demoEducation;
	private String demoIncome;
	private String demoOccupation;
	private String demoAddictions;
	private String demoFamilySize;
	private String imageUrl;
	private String bio;
}
