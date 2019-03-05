package com.qms.rest.controller;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.qms.rest.model.SMVMemberDetails;
import com.qms.rest.model.SMVMemberPayerClustering;
import com.qms.rest.model.SmvMemberClinical;
import com.qms.rest.service.SMVService;

@RestController
@RequestMapping("/smv")
@CrossOrigin
public class SMVController {
	
	@Autowired
	SMVService smvService;
	
	@RequestMapping(value = "/getSMVMemberDetails/{memberId}", method = RequestMethod.GET)
	public ResponseEntity<Set<SMVMemberDetails>> getSMVMemberDetails(@PathVariable("memberId") String memberId) {
		Set<SMVMemberDetails> workList = smvService.getSMVMemberDetails(memberId);
		if (workList==null) {
			return new ResponseEntity<Set<SMVMemberDetails>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<SMVMemberDetails>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getSmvMemberClinical/{memberId}", method = RequestMethod.GET)
	public ResponseEntity<Set<SmvMemberClinical>> getSmvMemberClinical(@PathVariable("memberId") String memberId) {
		Set<SmvMemberClinical> workList = smvService.getSmvMemberClinical(memberId);
		if (workList==null) {
			return new ResponseEntity<Set<SmvMemberClinical>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<SmvMemberClinical>>(workList, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/getSMVMemberPayerClustering/{memberId}", method = RequestMethod.GET)
	public ResponseEntity<Set<SMVMemberPayerClustering>> getSMVMemberPayerClustering(@PathVariable("memberId") String memberId) {
		Set<SMVMemberPayerClustering> workList = smvService.getSMVMemberPayerClustering(memberId);
		if (workList==null) {
			return new ResponseEntity<Set<SMVMemberPayerClustering>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<SMVMemberPayerClustering>>(workList, HttpStatus.OK);
	}	
}
