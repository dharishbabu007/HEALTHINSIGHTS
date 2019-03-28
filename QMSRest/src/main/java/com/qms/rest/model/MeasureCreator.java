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
public class MeasureCreator implements Comparable<MeasureCreator> {

	private int id;
	private String programName;
	private String name;
	private String description;
	private String measureDomain;
	private String measureCategory;
	private String type;
	private String clinocalCondition;
	private String targetAge;
	private String numerator;
	private String denominator;
	private String numeratorExclusions;
	private String denomExclusions;
	private String steward;
	private String dataSource;
	private String target;
	private String status;
	private String reviewComments;
	private String reviewedBy;
	private int measureEditId;
	private String sourceType;
	private String isActive;
	private String startDate;
	private String endDate;	
	
	private int p50;
	private int p90;
	private String collectionSource;
	private int mrss;
	private String overFlowRate;
	private int measurementYear;
	private String optionalExclusion;
	
	@Override
	public int compareTo(MeasureCreator arg0) {
		return Integer.compare(this.getId(), arg0.getId());
	}
}
