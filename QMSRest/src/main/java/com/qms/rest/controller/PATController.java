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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.http.HttpHeaders;

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
	
	@RequestMapping(value = "/search_associated_patient_list/{measureSK}/{mrnIdOrName}", method = RequestMethod.GET)
	public ResponseEntity<Set<SearchAssociatedPatient>> searchAssociatedPatientList(@PathVariable("measureSK") 
	String measureSK, @PathVariable("mrnIdOrName") String mrnIdOrName) {
		Set<SearchAssociatedPatient> workList = patService.searchAssociatedPatientList(measureSK, mrnIdOrName);
		if (workList.isEmpty()) {
			return new ResponseEntity<Set<SearchAssociatedPatient>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<SearchAssociatedPatient>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/action_on_caregap_list/{measureSK}", method = RequestMethod.GET)
	public ResponseEntity<PatActionCareGap> actionOnCareGapList(@PathVariable("measureSK") String measureSK) {
		PatActionCareGap workList = patService.actionOnCareGapList(measureSK);
		if (workList == null) {
			return new ResponseEntity<PatActionCareGap>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<PatActionCareGap>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_pat/{memberId}/{measureSK}", method = RequestMethod.GET)
	public ResponseEntity<List<Pat>> getPatById(@PathVariable("memberId") String memberId, @PathVariable("measureSK") String measureSK) {
		List<Pat> workList = patService.getPatById(memberId, measureSK);
		if (workList == null) {
			return new ResponseEntity<List<Pat>>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<List<Pat>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/member/{mid}", method = RequestMethod.GET)
	public ResponseEntity<DimMemeber> getMemberGapList(@PathVariable("mid") String mid) {
		DimMemeber dimMemberGaps = patService.findMembergapListByMid(mid);
		if (dimMemberGaps == null) {
			return new ResponseEntity(new CustomErrorType("dimMemberGaps with mid " + mid 
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

	@RequestMapping(value = "/pat_file_import/", method = RequestMethod.POST)
	public ResponseEntity<RestResult> importFile(@RequestParam("file") MultipartFile uploadfile) {
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Access-Control-Allow-Origin", "*");		
		headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
		
		if (uploadfile.isEmpty()) {            
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("File is empty. Please select a valid file!"), headers, 
            		HttpStatus.BAD_REQUEST);
        }
		
		//storing file data in linux 
		RestResult restResult = patService.importFile(uploadfile);				
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.OK);
		} else {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.INTERNAL_SERVER_ERROR);	
		}
	}	
}
