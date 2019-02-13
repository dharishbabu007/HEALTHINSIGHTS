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
public class Objectives {
	//private String objectiveId;
	private String title;
	private String metric;
	//private String metricValue;
	//private String yoy;
	//private String period;
	//private String periodValue;
	//private List<String> objectives = new ArrayList<>();
	private List<String> metricValueList = new ArrayList<>();
	private List<String> yoyList = new ArrayList<>();
	private List<String> periodList = new ArrayList<>();
	private List<String> periodValueList = new ArrayList<>();
	}
