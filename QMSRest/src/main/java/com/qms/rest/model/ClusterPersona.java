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
public class ClusterPersona {
	private int clusterId;
	private String personaName;
	private String demographics;
	private String motivations;
	private String goals;
	private String barriers;
	private String socialMedia;
	private String healthStatus;	
}
