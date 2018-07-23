package com.qms.rest.service;

import java.util.Set;

import com.qms.rest.model.DimPatient;
import com.qms.rest.model.MemberDetail;
import com.qms.rest.model.User;

public interface PatientService {
	
	DimPatient getPatientById(String patientId);
	
	DimPatient getMemberById(String memberId);
	
	Set<MemberDetail> getMemberDetails();
	
	User getUserInfo(String userName, String password);
}
