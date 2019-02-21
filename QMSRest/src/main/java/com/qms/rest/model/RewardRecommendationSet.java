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
public class RewardRecommendationSet {
	private String memberId;
	private String name;
	private String age;
	private String gender;
	private String persona;
	private RewardRecommendationSetData rewardRecommendation; 
	private RewardRecommendationSetData rewardSet;
}
