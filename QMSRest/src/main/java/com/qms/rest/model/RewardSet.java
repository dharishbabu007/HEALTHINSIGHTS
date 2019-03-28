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
public class RewardSet {
	private String category;
	private String goal;
	private String frequency; 
	private String goalDate;
	private String reward;
	private String interventions;
}
