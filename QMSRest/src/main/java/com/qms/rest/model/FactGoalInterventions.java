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
public class FactGoalInterventions {
	private String goalSetId;
	private String memberId;
	private String name;
	private String age;
	private String gender;
	private String persona;
	private String preferredGoal;
	private String weight;
	private String currentCalorieIntake;
	private String numberOfChronicDiseases;
	private String addictions;
	private String physicalActivityId;
	private String physicalActivityGoal;
	private String physicalActivityFrequency;
	private String physicalActivityDate;
	private String calorieIntakeId;
	private String calorieIntakeGoal;
	private String calorieIntakeFrequency;
	private String calorieIntakeDate;
	private String qualityMeasureId;
	private String careGap;
	private String careGapDate;
	private String interventions;
	private String currFlag;
	private String recCreateDate;
	private String recUpdateDate;
	private String latestFlag;
	private String activeFlag;
	private String ingestionDate;
	private String sourceName;
	private String userName;
}
