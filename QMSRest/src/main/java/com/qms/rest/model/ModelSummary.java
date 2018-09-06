package com.qms.rest.model;

public class ModelSummary {
	private String attributes;
	private String estimate;
	private String stdError;
	private String zValue;
	private String prz;
	private String significance;
	
	public String getAttributes() {
		return attributes;
	}
	public void setAttributes(String attributes) {
		this.attributes = attributes;
	}
	public String getEstimate() {
		return estimate;
	}
	public void setEstimate(String estimate) {
		this.estimate = estimate;
	}
	public String getStdError() {
		return stdError;
	}
	public void setStdError(String stdError) {
		this.stdError = stdError;
	}
	public String getzValue() {
		return zValue;
	}
	public void setzValue(String zValue) {
		this.zValue = zValue;
	}
	public String getPrz() {
		return prz;
	}
	public void setPrz(String prz) {
		this.prz = prz;
	}
	public String getSignificance() {
		return significance;
	}
	public void setSignificance(String significance) {
		this.significance = significance;
	}
	
	
}
