package com.qms.rest.service;

import com.qms.rest.model.DimPatient;
import com.qms.rest.model.User;

public interface PatientService {
	
	DimPatient getPatientById(String patientId);
	
	DimPatient getMemberById(String memberId);
	
	User getUserInfo(String userName, String password);
}
