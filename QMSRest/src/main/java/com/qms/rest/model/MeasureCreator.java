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
	//private String collectionMethod;
	private int mrss;
	private String overFlowRate;
	
//	public String getSourceType() {
//		return sourceType;
//	}
//	public void setSourceType(String sourceType) {
//		this.sourceType = sourceType;
//	}
//	public String getDenomExclusions() {
//		return denomExclusions;
//	}
//	public void setDenomExclusions(String denomExclusions) {
//		this.denomExclusions = denomExclusions;
//	}
//	public int getId() {
//		return id;
//	}
//	public void setId(int id) {
//		this.id = id;
//	}
//	public String getProgramName() {
//		return programName;
//	}
//	public void setProgramName(String programName) {
//		this.programName = programName;
//	}
//	public String getName() {
//		return name;
//	}
//	public void setName(String name) {
//		this.name = name;
//	}
//	public String getDescription() {
//		return description;
//	}
//	public void setDescription(String description) {
//		this.description = description;
//	}
//	public String getMeasureDomain() {
//		return measureDomain;
//	}
//	public void setMeasureDomain(String measureDomain) {
//		this.measureDomain = measureDomain;
//	}
//	public String getMeasureCategory() {
//		return measureCategory;
//	}
//	public void setMeasureCategory(String measureCategory) {
//		this.measureCategory = measureCategory;
//	}
//	public String getType() {
//		return type;
//	}
//	public void setType(String type) {
//		this.type = type;
//	}
//	public String getClinocalCondition() {
//		return clinocalCondition;
//	}
//	public void setClinocalCondition(String clinocalCondition) {
//		this.clinocalCondition = clinocalCondition;
//	}
//	public String getTargetAge() {
//		return targetAge;
//	}
//	public void setTargetAge(String targetAge) {
//		this.targetAge = targetAge;
//	}
//	public String getNumerator() {
//		return numerator;
//	}
//	public void setNumerator(String numerator) {
//		this.numerator = numerator;
//	}
//	public String getDenominator() {
//		return denominator;
//	}
//	public void setDenominator(String denominator) {
//		this.denominator = denominator;
//	}
//	public String getNumeratorExclusions() {
//		return numeratorExclusions;
//	}
//	public void setNumeratorExclusions(String numeratorExclusions) {
//		this.numeratorExclusions = numeratorExclusions;
//	}
//	public String getSteward() {
//		return steward;
//	}
//	public void setSteward(String steward) {
//		this.steward = steward;
//	}
//	public String getDataSource() {
//		return dataSource;
//	}
//	public void setDataSource(String dataSource) {
//		this.dataSource = dataSource;
//	}
//	public String getTarget() {
//		return target;
//	}
//	public void setTarget(String target) {
//		this.target = target;
//	}
//	public String getStatus() {
//		return status;
//	}
//	public void setStatus(String status) {
//		this.status = status;
//	}
//	public String getReviewComments() {
//		return reviewComments;
//	}
//	public void setReviewComments(String reviewComments) {
//		this.reviewComments = reviewComments;
//	}
//	public String getReviewedBy() {
//		return reviewedBy;
//	}
//	public void setReviewedBy(String reviewedBy) {
//		this.reviewedBy = reviewedBy;
//	}
//	public int getMeasureEditId() {
//		return measureEditId;
//	}
//	public void setMeasureEditId(int measureEditId) {
//		this.measureEditId = measureEditId;
//	}
//	public String getIsActive() {
//		return isActive;
//	}
//	public void setIsActive(String isActive) {
//		this.isActive = isActive;
//	}
//	public String getStartDate() {
//		return startDate;
//	}
//	public void setStartDate(String startDate) {
//		this.startDate = startDate;
//	}
//	public String getEndDate() {
//		return endDate;
//	}
//	public void setEndDate(String endDate) {
//		this.endDate = endDate;
//	}
	@Override
	public int compareTo(MeasureCreator arg0) {
		return Integer.compare(this.getId(), arg0.getId());
	}
	
	
	
}
