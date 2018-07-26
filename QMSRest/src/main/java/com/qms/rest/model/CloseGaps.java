package com.qms.rest.model;

import java.util.Set;

public class CloseGaps {
	private String memberId;
	private String gender;
	private String name;
	private String dateOfBirth;
	private Set<CloseGap> careGaps;
	
	public String getMemberId() {
		return memberId;
	}
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDateOfBirth() {
		return dateOfBirth;
	}
	public void setDateOfBirth(String dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}
	public Set<CloseGap> getCareGaps() {
		return careGaps;
	}
	public void setCareGaps(Set<CloseGap> careGaps) {
		this.careGaps = careGaps;
	}
}
