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
public class FactGoalRecommendations {

	private String recommendationId;
	private String memberId;
	private String name;
	private String age;
	private String gender;
	private String physicalActivityGoal;
	private String physicalActivityFrequency;
	private String physicalActivityDate;
	private String calorieIntakeGoal;
	private String calorieIntakeFrequency;
	private String calorieIntakeDate;
	private String careGap;
	private String careGapDate;
	private String persona;
	private String preferredGoal;
	private String currentCalorieIntake;
	private String numberOfChronicDiseases;
	private String addictions;
	private String physicalActivityId;
	private String calorieIntakeId;
	private String qualityMeasureId;

}
