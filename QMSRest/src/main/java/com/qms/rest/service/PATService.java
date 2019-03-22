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
	Set<NameValue> getPopulationList();
	Set<NameValue> getCareGapList();
	Set<SearchAssociatedPatient> searchAssociatedPatientList(String measureId, String mrnIdOrName);
	PatActionCareGap actionOnCareGapList(String measureId);	
	List<Pat> getPatById(String patId, String measureId);
	DimMemeber findMembergapListByMid(String memberId);
	RestResult insertPatCreator(Pat pat);
}
