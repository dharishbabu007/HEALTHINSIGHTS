package com.qms.rest.model;

public class CloseGap {
	private String measureTitle;
	private String qualityMeasureId;
	private String intervention;
	private String priority;
	private String payerComments;
	private String providerComments;
	private String status;
	private String dateTime;
	
	public String getPayerComments() {
		return payerComments;
	}
	public void setPayerComments(String payerComments) {
		this.payerComments = payerComments;
	}
	public String getProviderComments() {
		return providerComments;
	}
	public void setProviderComments(String providerComments) {
		this.providerComments = providerComments;
	}
	public String getMeasureTitle() {
		return measureTitle;
	}
	public void setMeasureTitle(String measureTitle) {
		this.measureTitle = measureTitle;
	}
	public String getQualityMeasureId() {
		return qualityMeasureId;
	}
	public void setQualityMeasureId(String qualityMeasureId) {
		this.qualityMeasureId = qualityMeasureId;
	}
	public String getIntervention() {
		return intervention;
	}
	public void setIntervention(String intervention) {
		this.intervention = intervention;
	}
	public String getPriority() {
		return priority;
	}
	public void setPriority(String priority) {
		this.priority = priority;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}		
}
