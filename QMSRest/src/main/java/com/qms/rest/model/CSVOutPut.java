package com.qms.rest.model;

public class CSVOutPut {
	
	private String patientId;
	private String patientName;
	private String appointmentID;
	private String scheduledDay;
	private String appointmentDay;
	private String neighbourhood;
	private String likelihood;
	private String noShow;
	
	public String getPatientId() {
		return patientId;
	}
	public void setPatientId(String patientId) {
		this.patientId = patientId;
	}
	public String getAppointmentID() {
		return appointmentID;
	}
	public void setAppointmentID(String appointmentID) {
		this.appointmentID = appointmentID;
	}
	public String getScheduledDay() {
		return scheduledDay;
	}
	public void setScheduledDay(String scheduledDay) {
		this.scheduledDay = scheduledDay;
	}
	public String getAppointmentDay() {
		return appointmentDay;
	}
	public void setAppointmentDay(String appointmentDay) {
		this.appointmentDay = appointmentDay;
	}
	public String getNeighbourhood() {
		return neighbourhood;
	}
	public void setNeighbourhood(String neighbourhood) {
		this.neighbourhood = neighbourhood;
	}
	public String getLikelihood() {
		return likelihood;
	}
	public void setLikelihood(String likelihood) {
		this.likelihood = likelihood;
	}
	public String getNoShow() {
		return noShow;
	}
	public void setNoShow(String noShow) {
		this.noShow = noShow;
	}
	public String getPatientName() {
		return patientName;
	}
	public void setPatientName(String patientName) {
		this.patientName = patientName;
	}
	
	
}
