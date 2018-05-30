package com.qms.rest.model;

public class RestResult {
	
	private String status;
	private String message;
	
	public static String SUCCESS_STATUS = "SUCCESS";
	public static String FAIL_STATUS = "FAIL";
	
	public static RestResult getRestResult(String status, String message) {
		RestResult restResult = new RestResult();
		restResult.setMessage(message);
		restResult.setStatus(status);
		return restResult;
	}
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
}
