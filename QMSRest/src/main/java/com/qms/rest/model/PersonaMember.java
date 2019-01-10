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
public class PersonaMember {
	private String memberID;	
	private String memberName;	
	private String age;	
	private String gender;	
	private String formExercise;	
	private String frequencyExercise;	
	private String motivation;	
	private String likelihoodEnrollment;	
	private String reasonToNotEnroll;	
	private String setAchieveHealthGo;	
	private String familySize;	
	private String clusterID;	
	private String personaName;
}
