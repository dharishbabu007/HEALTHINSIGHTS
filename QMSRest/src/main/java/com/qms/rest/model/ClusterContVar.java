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
public class ClusterContVar {
	private String sno;
	private String clusterId;
	private String attribute;
	private String min;
	private String firstQuartile;
	private String median;
	private String secondQuartile;
	private String max;
}
