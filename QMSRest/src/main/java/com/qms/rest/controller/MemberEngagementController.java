package com.qms.rest.controller;

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

import com.qms.rest.model.ClusterAnalysis;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.ClusterPersona;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.MemberEngagementService;

@RestController
@RequestMapping("/member_engagement")
@CrossOrigin
public class MemberEngagementController {

	@Autowired
	MemberEngagementService memberEngagementService;	
	
	@RequestMapping(value = "/modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getModelSummaryCSVData() {
		Set<ModelSummary> setCSVOutPut = memberEngagementService.getCSVModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/confusionMatric", method = RequestMethod.GET)
	public ResponseEntity<Set<ConfusionMatric>> getConfusionMatricCSVData() {
		Set<ConfusionMatric> setCSVOutPut = memberEngagementService.getCSVConfusionMatric();
		return new ResponseEntity<Set<ConfusionMatric>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/modelScore", method = RequestMethod.GET)
	public ResponseEntity<ModelScore> getModelScoreCSVData() {
		ModelScore cSVOutPut = memberEngagementService.getCSVModelScore();
		return new ResponseEntity<ModelScore>(cSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/clusterAnalysis", method = RequestMethod.GET)
	public ResponseEntity<Set<ClusterAnalysis>> getClusterAnalysis() {
		Set<ClusterAnalysis> setCSVOutPut = memberEngagementService.getCSVClusterAnalysis();
		return new ResponseEntity<Set<ClusterAnalysis>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/clusterData/{clusterId}", method = RequestMethod.GET)
	public ResponseEntity<ClusterData> getClusterData(@PathVariable("clusterId") int clusterId) {
		ClusterData setCSVOutPut = memberEngagementService.getClusteringData(clusterId);
		if(setCSVOutPut == null) {
			return new ResponseEntity<ClusterData>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<ClusterData>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/update_persona", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateClusteringPersona(@RequestBody ClusterPersona clusterPersona, 
			UriComponentsBuilder ucBuilder) {
		RestResult restResult = memberEngagementService.updateClusteringPersona(clusterPersona);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/lhe_output", method = RequestMethod.GET)
	public ResponseEntity<Set<LHEOutput>> getLHEOutput() {
		System.out.println("Fetching LHE out ");
		Set<LHEOutput> setCSVOutPut = memberEngagementService.getLHEModelOutPut();
		return new ResponseEntity<Set<LHEOutput>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhe_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getLHEModelSummary() {
		System.out.println("Fetching LHE Model summary ");
		Set<ModelSummary> setCSVOutPut = memberEngagementService.getLHEModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhe_modelMatric", method = RequestMethod.GET)
	public ResponseEntity<ModelMetric> getLHEModelMatric() {
		System.out.println("Fetching LHE Model metric ");
		ModelMetric setCSVOutPut = memberEngagementService.getLHEModelMetric();
		return new ResponseEntity<ModelMetric>(setCSVOutPut, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/lhe_reason_not_enroll_statics", method = RequestMethod.GET)
	public ResponseEntity<String[][]> getLHEReasonNotEnrollStatics() {
		System.out.println("Fetching LHE Reason Not Enroll Statics ");
		String[][] setCSVOutPut = memberEngagementService.getLHEReasonNotEnrollStatics();
		return new ResponseEntity<String[][]>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/create_lhe_input_file", method = RequestMethod.GET)
	public ResponseEntity<RestResult> createLHEInputFile() {
		System.out.println("Create LHE Input File ");
		RestResult restResult = memberEngagementService.createLHEInputFile();
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
}
