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
public class LhrMemberListView {
	private String name;
	private String member_id;
	private String age;
	private String gender;
	private String persona;
	private String motivations;
	private String physicalActivityGoal;
	private String physicalActivityFrequency;
	private String physicalActivityDuration;
	private String calorieIintakeGoal;
	private String calorieIntakeFrequency;
	private String calorieIntakeDuration;
	private String careGap;
	private String careGapDuration;
	private String performancePhysicalActivity;
	private String performanceCalorieIntake;
	private String performanceCareGap;
	private String education;
	private String income;
	private String familySize;
	private String likelihoodToRecommend;
	private String recommendation;
}
