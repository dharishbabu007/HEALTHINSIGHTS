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
public class QmsReffReward {
	private String rewardId;;
	private String rewardl;
	private String latestFlag;
	private String activeFlag;
	private String ingestionDate;
	private String sourceName;
	private String userName;
}
