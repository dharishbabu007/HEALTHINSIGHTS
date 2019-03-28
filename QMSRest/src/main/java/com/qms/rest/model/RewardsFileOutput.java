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

public class RewardsFileOutput {	
	private String rewardId;
	private String memberId;
	private String name;
	private String age;
	private String gender;
	private String weight;
	private String persona;
	private String preferredReward;
	private String motivations;
	private String category;
	private String goal;
	private String frequency;
	private String goalDate;
	private String reward1;
	private String reward2;
	private String reward3;
	private String recommendedReward;
	//private String others;
	
}
