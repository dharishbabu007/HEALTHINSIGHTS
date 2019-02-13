package com.qms.rest.model;


public class RefMrss {
	
	private String mrssId ; 
	private String measureName; 
	private String measureCategary; 
	private String medicaid ;
	private String commercial;
	private String medicare;
	private String sample_size;
	private String rand;
	
	public String getMrssId() {
		return mrssId;
	}
	public void setMrssId(String mrssId) {
		this.mrssId = mrssId;
	}
	public String getMeasureName() {
		return measureName;
	}
	public void setMeasureName(String measureName) {
		this.measureName = measureName;
	}
	public String getMeasureCategary() {
		return measureCategary;
	}
	public void setMeasureCategary(String measureCategary) {
		this.measureCategary = measureCategary;
	}
	public String getMedicaid() {
		return medicaid;
	}
	public void setMedicaid(String medicaid) {
		this.medicaid = medicaid;
	}
	public String getCommercial() {
		return commercial;
	}
	public void setCommercial(String commercial) {
		this.commercial = commercial;
	}
	public String getMedicare() {
		return medicare;
	}
	public void setMedicare(String medicare) {
		this.medicare = medicare;
	}
	public String getSample_size() {
		return sample_size;
	}
	public void setSample_size(String sample_size) {
		this.sample_size = sample_size;
	}
	public String getRand() {
		return rand;
	}
	public void setRand(String rand) {
		this.rand = rand;
	}
	
}
