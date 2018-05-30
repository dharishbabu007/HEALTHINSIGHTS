package com.qms.rest.model;

public class MeasureConfig {
	
	//private String tableId;
	private String measureId;
	private String category;
	private int categoryLineId;
	private String operator;
	private String businessExpression;
	private String technicalExpression;
	private String remarks;	
	private String status;
	private int version;
	private String modifiedBy;
	private String modifiedDate;
	
	public String getMeasureId() {
		return measureId;
	}
	public void setMeasureId(String measureId) {
		this.measureId = measureId;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public int getCategoryLineId() {
		return categoryLineId;
	}
	public void setCategoryLineId(int categoryLineId) {
		this.categoryLineId = categoryLineId;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public String getBusinessExpression() {
		return businessExpression;
	}
	public void setBusinessExpression(String businessExpression) {
		this.businessExpression = businessExpression;
	}
	public String getTechnicalExpression() {
		return technicalExpression;
	}
	public void setTechnicalExpression(String technicalExpression) {
		this.technicalExpression = technicalExpression;
	}
	public String getRemarks() {
		return remarks;
	}
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getModifiedBy() {
		return modifiedBy;
	}
	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}
	public String getModifiedDate() {
		return modifiedDate;
	}
	public void setModifiedDate(String modifiedDate) {
		this.modifiedDate = modifiedDate;
	}	
}
