package com.qms.rest.model;

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
public class QmsGicLifecycle {

	private List<FactHedisGapsInCare> factHedisGapsInCare;
	private String interventions;
	private String priority;
	private String payorComments;
	private String providerComments;
	private String status;
	private String qualityMeasureId;
	private String duration;
	private String start_date;
	private String end_date;
	private String MeasureTitle;
}
