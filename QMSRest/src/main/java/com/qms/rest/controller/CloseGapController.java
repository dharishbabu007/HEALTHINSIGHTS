package com.qms.rest.controller;

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

import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.CloseGapsService;

@RestController
@RequestMapping("/closeGaps")
@CrossOrigin
public class CloseGapController {

	@Autowired
	CloseGapsService closeGapsService;	
	
	@RequestMapping(value = "/{memberId}/{measureId}", method = RequestMethod.GET)
	public ResponseEntity<CloseGaps> getCloseGaps(@PathVariable("memberId") String memberId, 
			@PathVariable("measureId") String measureId) {
		CloseGaps closeGaps = closeGapsService.getCloseGaps(memberId, measureId);
		if (closeGaps == null) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<CloseGaps>(closeGaps, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/{memberId}/{measureId}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> insertCloseGaps(@PathVariable("memberId") String memberId, 
			@PathVariable("measureId") String measureId,
			@RequestBody CloseGaps closeGaps, UriComponentsBuilder ucBuilder) {
		RestResult restResult = closeGapsService.insertCloseGaps(closeGaps, memberId, measureId);
		return new ResponseEntity<RestResult>(restResult, HttpStatus.CREATED);
	}	
}
