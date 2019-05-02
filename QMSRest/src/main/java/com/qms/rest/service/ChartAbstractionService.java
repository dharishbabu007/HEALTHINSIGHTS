package com.qms.rest.service;

import java.util.List;
import java.util.Set;

import com.qms.rest.model.ChartAbs;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.Patient;
import com.qms.rest.model.RestResult;

public interface ChartAbstractionService {
	Patient getPatientDetails(String patientId);

	List<DimMemberGapListSearch> findSearchPatientList(String search);
	
	List<ChartAbs> getChartAbs(String patientId, String chartType, int visitId);
	
	List<ChartAbs> getChartAbsVisits(String patientId);	
	
	RestResult addChartAbs(List<ChartAbs> chartAbs);
	
	Set<String> getEncounterTypes();

	List<ChartAbs> getChartAbsHistory(String patientId, String chartType);
}
