package com.qms.rest.model;

import java.util.HashSet;
import java.util.List;
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
public class PersonaMemberListView {
	private String memberId;
	private String personaName;
	private String goals;
	private String measureCalorieIntake;
	private String comorbidityCount;
	private String addictions;
	private String rewards;
	private String motivations;
	
	private String modeOfContact;    
	private String formOfExercise;      
	private String frequencyOfExcercise;       
	private String idealHealthGoal;
	private String socialMediaUsage;                
	private String age;      
	private String gender;
	private String ethnicity;
	private String clusterId;       		
}
