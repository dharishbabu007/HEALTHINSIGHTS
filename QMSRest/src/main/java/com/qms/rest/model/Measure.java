package com.qms.rest.model;

public class Measure {

	private int id;
	private String name;
	private String programName;
	private String type;
	private String steward;
	private String clinocalCondition;
	private String status;
	private String reviewComments;
	private String reviewedBy;
	private String targetAge;
	private String measureDomain;
	private String measureCategory;
	private String target;
	private String description;
	private String denominator;
	private String denomExclusions;
	private String numerator;
	private String numeratorExclusions;
	
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getProgramName() {
		return programName;
	}
	public void setProgramName(String programName) {
		this.programName = programName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSteward() {
		return steward;
	}
	public void setSteward(String steward) {
		this.steward = steward;
	}
	public String getClinocalCondition() {
		return clinocalCondition;
	}
	public void setClinocalCondition(String clinocalCondition) {
		this.clinocalCondition = clinocalCondition;
	}	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getReviewComments() {
		return reviewComments;
	}
	public void setReviewComments(String reviewComments) {
		this.reviewComments = reviewComments;
	}
	public String getReviewedBy() {
		return reviewedBy;
	}
	public void setReviewedBy(String reviewedBy) {
		this.reviewedBy = reviewedBy;
	}	
	
	public String getTargetAge() {
		return targetAge;
	}
	public void setTargetAge(String targetAge) {
		this.targetAge = targetAge;
	}
	public String getMeasureDomain() {
		return measureDomain;
	}
	public void setMeasureDomain(String measureDomain) {
		this.measureDomain = measureDomain;
	}
	public String getMeasureCategory() {
		return measureCategory;
	}
	public void setMeasureCategory(String measureCategory) {
		this.measureCategory = measureCategory;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDenominator() {
		return denominator;
	}
	public void setDenominator(String denominator) {
		this.denominator = denominator;
	}
	public String getDenomExclusions() {
		return denomExclusions;
	}
	public void setDenomExclusions(String denomExclusions) {
		this.denomExclusions = denomExclusions;
	}
	public String getNumerator() {
		return numerator;
	}
	public void setNumerator(String numerator) {
		this.numerator = numerator;
	}
	public String getNumeratorExclusions() {
		return numeratorExclusions;
	}
	public void setNumeratorExclusions(String numeratorExclusions) {
		this.numeratorExclusions = numeratorExclusions;
	}	
}
