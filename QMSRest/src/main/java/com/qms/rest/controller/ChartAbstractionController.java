package com.qms.rest.controller;

import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.ChartAbs;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.Patient;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.ChartAbstractionService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/chart_abstraction")
@CrossOrigin
public class ChartAbstractionController {
	
	@Autowired
	ChartAbstractionService chartAbstractionService;
	
	@RequestMapping(value = "/getPatientDetails/{patientId}", method = RequestMethod.GET)
	public ResponseEntity<Patient> getPatientDetails(@PathVariable("patientId") String patientId) {
		Patient setCSVOutPut = chartAbstractionService.getPatientDetails(patientId);
		if (setCSVOutPut == null) {
			return new ResponseEntity<Patient>(setCSVOutPut, HttpStatus.NO_CONTENT);
		}		
		return new ResponseEntity<Patient>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/searchPatient/{search}", method = RequestMethod.GET)
	public ResponseEntity<List<DimMemberGapListSearch>> findSearchPatientList(@PathVariable("search") String search) {
		search = search.substring(0, 1).toUpperCase()+search.substring(1);
		List<DimMemberGapListSearch> dimMemberGapListSearch = chartAbstractionService.findSearchPatientList(search);
		if (dimMemberGapListSearch == null || dimMemberGapListSearch.isEmpty()) {
			return new ResponseEntity<List<DimMemberGapListSearch>>(dimMemberGapListSearch, HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<DimMemberGapListSearch>>((List<DimMemberGapListSearch>) dimMemberGapListSearch, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getChartAbs/{patientId}/{visitId}/{chartType}", method = RequestMethod.GET)
	public ResponseEntity<List<ChartAbs>> getChartAbs(@PathVariable("patientId") String patientId,
			@PathVariable("chartType") String chartType, @PathVariable("visitId") int visitId) {
		List<ChartAbs> setCSVOutPut = chartAbstractionService.getChartAbs(patientId,chartType,visitId);
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getChartAbsHistory/{patientId}/{chartType}", method = RequestMethod.GET)
	public ResponseEntity<List<ChartAbs>> getChartAbsHistory(@PathVariable("patientId") String patientId,
			@PathVariable("chartType") String chartType) {
		List<ChartAbs> setCSVOutPut = chartAbstractionService.getChartAbsHistory(patientId,chartType);
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/getChartAbsVisits/{patientId}", method = RequestMethod.GET)
	public ResponseEntity<List<ChartAbs>> getChartAbsVisits(@PathVariable("patientId") String patientId) {
		List<ChartAbs> setCSVOutPut = chartAbstractionService.getChartAbsVisits(patientId);
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<ChartAbs>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/addChartAbs", method = RequestMethod.POST)
	public ResponseEntity<RestResult> addChartAbs(@RequestBody List<ChartAbs> chartAbs, UriComponentsBuilder ucBuilder) {
		RestResult restResult = chartAbstractionService.addChartAbs(chartAbs);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);		
	}
	
	@RequestMapping(value = "/getEncounterTypes", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getEncounterTypes() {
		Set<String> setCSVOutPut = chartAbstractionService.getEncounterTypes();
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<Set<String>>(setCSVOutPut, HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<String>>(setCSVOutPut, HttpStatus.OK);
	}	
	
}
