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
public class CPOutput {
	String memberName;
	String memberId;
	String age;
	String daysPendingTermination;
	String outPocketExpenses;
	String formExercise;
	String frequencyExercise;
	String measureCalorieIntake;
	String dailyConsumption;
	String weight;
	String motivations;
	String amountSpend;
	String clusterId;

}
