package com.qms.rest.service;

import java.util.List;
import java.util.Set;

import com.qms.rest.model.DimMemeber;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Pat;
import com.qms.rest.model.PatActionCareGap;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.SearchAssociatedPatient;

public interface PATService {

	Set<String> getPopulationList();
	Set<NameValue> getCareGapList();
	Set<SearchAssociatedPatient> searchAssociatedPatientList(String measureSK, String mrnIdOrName);
	PatActionCareGap actionOnCareGapList(String measureSK);	
	List<Pat> getPatById(String patId, String measureSK);
	DimMemeber findMembergapListByMid(String mid);
	
	RestResult insertPatCreator(Pat pat);
//	RestResult importFile(MultipartFile file);
//	RestResult saveFileUpload(PatFileUpload fileUpload);	
	
}
