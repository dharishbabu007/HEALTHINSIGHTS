package com.qms.rest.controller;

import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.DimMemeber;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.Pat;
import com.qms.rest.model.PatActionCareGap;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.SearchAssociatedPatient;
import com.qms.rest.service.PATService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/pat")
@CrossOrigin
public class PATController {
	
	@Autowired
	PATService patService;
	
	@RequestMapping(value = "/get_population_list", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getPopulationList() {
		Set<String> workList = patService.getPopulationList();
		if (workList.isEmpty()) {
			return new ResponseEntity<Set<String>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_care_gap_list", method = RequestMethod.GET)
	public ResponseEntity<Set<NameValue>> getCareGapList() {
		Set<NameValue> workList = patService.getCareGapList();
		if (workList.isEmpty()) {
			return new ResponseEntity<Set<NameValue>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<NameValue>>(workList, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/search_associated_patient_list/{measureId}/{mrnIdOrName}", method = RequestMethod.GET)
	public ResponseEntity<Set<SearchAssociatedPatient>> searchAssociatedPatientList(@PathVariable("measureId") 
	String measureId, @PathVariable("mrnIdOrName") String mrnIdOrName) {
		Set<SearchAssociatedPatient> workList = patService.searchAssociatedPatientList(measureId, mrnIdOrName);
		if (workList.isEmpty()) {
			return new ResponseEntity<Set<SearchAssociatedPatient>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<SearchAssociatedPatient>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/action_on_caregap_list/{measureId}", method = RequestMethod.GET)
	public ResponseEntity<PatActionCareGap> actionOnCareGapList(@PathVariable("measureId") String measureId) {
		PatActionCareGap workList = patService.actionOnCareGapList(measureId);
		if (workList == null) {
			return new ResponseEntity<PatActionCareGap>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<PatActionCareGap>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_pat/{memberId}/{measureId}", method = RequestMethod.GET)
	public ResponseEntity<List<Pat>> getPatById(@PathVariable("memberId") String memberId, @PathVariable("measureId") String measureId) {
		List<Pat> workList = patService.getPatById(memberId, measureId);
		if (workList == null) {
			return new ResponseEntity<List<Pat>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<List<Pat>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/member/{memberId}", method = RequestMethod.GET)
	public ResponseEntity<DimMemeber> getMemberGapList(@PathVariable("memberId") String memberId) {
		DimMemeber dimMemberGaps = patService.findMembergapListByMid(memberId);
		if (dimMemberGaps == null) {
			return new ResponseEntity(new CustomErrorType("dimMemberGaps with mid " + memberId 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity((DimMemeber) dimMemberGaps, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/pat_create/", method = RequestMethod.POST)
	public ResponseEntity<RestResult> createPatCreater(@RequestBody Pat pat, UriComponentsBuilder ucBuilder) {
		//logger.info("Creating PatCreator : {}", pat);
		System.out.println(" Creating PatCreator with status --> " + pat.getPatientId());
		RestResult restResult = patService.insertPatCreator(pat);

		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path("/api/pat/{id}").buildAndExpand(10).toUri());
		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.CREATED);
	}	

}
