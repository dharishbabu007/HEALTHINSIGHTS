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

import com.qms.rest.model.CPOutput;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.PersonaDefine;
import com.qms.rest.model.PersonaMember;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.GraphData;
import com.qms.rest.model.LHCMember;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.Param;
import com.qms.rest.model.PersonaClusterFeatures;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleLandingPage;
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
	public ResponseEntity<String[][]> getClusterAnalysis() {
		String[][] setCSVOutPut = memberEngagementService.getCSVClusterAnalysis();
		return new ResponseEntity<String[][]>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/clusterData/{clusterId}", method = RequestMethod.GET)
	public ResponseEntity<ClusterData> getClusterData(@PathVariable("clusterId") String clusterId) {
		ClusterData setCSVOutPut = memberEngagementService.getClusteringData(clusterId);
		if(setCSVOutPut == null) {
			return new ResponseEntity<ClusterData>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<ClusterData>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/update_persona", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateClusteringPersona(@RequestBody PersonaDefine clusterPersona, 
			UriComponentsBuilder ucBuilder) {
		RestResult restResult = memberEngagementService.updateClusteringPersona(clusterPersona);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/persona_cluster_features_graph_data", method = RequestMethod.POST)
	public ResponseEntity<GraphData> getPersonaClusterGraphData(@RequestBody Param param) {	
		String clusterId = param.getValue1();
		String attributeName =  param.getValue2();
		GraphData setCSVOutPut = memberEngagementService.getPersonaClusterFeaturesGraphData(clusterId, attributeName);
		if(setCSVOutPut == null) {
			return new ResponseEntity<GraphData>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<GraphData>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/persona_cluster_features_data/{clusterId}", method = RequestMethod.GET)
	public ResponseEntity<Set<PersonaClusterFeatures>> getPersonaClusterFeatures(@PathVariable("clusterId") String clusterId) {	
		Set<PersonaClusterFeatures> setCSVOutPut = memberEngagementService.getPersonaClusterFeatures(clusterId);
		if(setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<Set<PersonaClusterFeatures>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Set<PersonaClusterFeatures>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/persona_cluster_names_list", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getPersonaClusterFeatures() {	
		Set<String> setCSVOutPut = memberEngagementService.getPersonaClusterNames();
		if(setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<Set<String>>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Set<String>>(setCSVOutPut, HttpStatus.OK);
	}
	
	/////////////////////////////LHE//////////////////////////////////////////
	@RequestMapping(value = "/lhe_output", method = RequestMethod.GET)
	public ResponseEntity<Set<LHEOutput>> getLHEOutput() {
		Set<LHEOutput> setCSVOutPut = memberEngagementService.getLHEModelOutPut();
		return new ResponseEntity<Set<LHEOutput>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhe_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getLHEModelSummary() {
		Set<ModelSummary> setCSVOutPut = memberEngagementService.getLHEModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhe_modelMatric", method = RequestMethod.GET)
	public ResponseEntity<ModelMetric> getLHEModelMatric() {
		ModelMetric setCSVOutPut = memberEngagementService.getLHEModelMetric();
		return new ResponseEntity<ModelMetric>(setCSVOutPut, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/lhe_reason_not_enroll_statics", method = RequestMethod.GET)
	public ResponseEntity<String[][]> getLHEReasonNotEnrollStatics() {
		String[][] setCSVOutPut = memberEngagementService.getLHEReasonNotEnrollStatics();
		return new ResponseEntity<String[][]>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/create_lhe_input_file", method = RequestMethod.GET)
	public ResponseEntity<RestResult> createLHEInputFile() {
		RestResult restResult = memberEngagementService.createLHEInputFile();
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	/////////////////////////////PERSONA//////////////////////////////////////////	
	@RequestMapping(value = "/create_persona_input_file", method = RequestMethod.GET)
	public ResponseEntity<RestResult> createPersonaInputFile() {
		RestResult restResult = memberEngagementService.createPersonaInputFile();
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	@RequestMapping(value = "/persona_member_list/{clusterId}", method = RequestMethod.GET)
	public ResponseEntity<Set<PersonaMember>> personaMemberList(@PathVariable("clusterId") String clusterId) {
		Set<PersonaMember> setCSVOutPut = memberEngagementService.personaMemberList(clusterId);
		return new ResponseEntity<Set<PersonaMember>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getCPFeature", method = RequestMethod.GET)
	public ResponseEntity<List<PersonaClusterFeatures>> getCPFeature() {
		List<PersonaClusterFeatures> setCSVOutPut = memberEngagementService.getCPFeature();
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<List<PersonaClusterFeatures>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<PersonaClusterFeatures>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/getCPOutput", method = RequestMethod.GET)
	public ResponseEntity<Set<CPOutput>> getCPOutput() {
		Set<CPOutput> setCSVOutPut = memberEngagementService.getCPOutput();
		if (setCSVOutPut == null || setCSVOutPut.isEmpty()) {
			return new ResponseEntity<Set<CPOutput>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<CPOutput>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getCPStatistics", method = RequestMethod.GET)
	public ResponseEntity<String[][]> getCPStatistics() {
		String[][] setCSVOutPut = memberEngagementService.getCPStatistics();
		return new ResponseEntity<String[][]>(setCSVOutPut, HttpStatus.OK);
	}	
	
	/////////////////////////////LHC//////////////////////////////////////////	
	@RequestMapping(value = "/create_lhc_input_file", method = RequestMethod.GET)
	public ResponseEntity<RestResult> createLHCInputFile() {
		RestResult restResult = memberEngagementService.createLHCInputFile();
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	@RequestMapping(value = "/lhc_member_list", method = RequestMethod.GET)
	public ResponseEntity<Set<LHCMember>> lhcMemberList() {
		Set<LHCMember> setCSVOutPut = memberEngagementService.lhcMemberList();
		return new ResponseEntity<Set<LHCMember>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhc_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getLHCModelSummary() {
		Set<ModelSummary> setCSVOutPut = memberEngagementService.getLHCModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/lhc_modelMatric", method = RequestMethod.GET)
	public ResponseEntity<ModelMetric> getLHCModelMatric() {
		ModelMetric setCSVOutPut = memberEngagementService.getLHCModelMetric();
		return new ResponseEntity<ModelMetric>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/role_landing_page", method = RequestMethod.GET)
	public ResponseEntity<Set<RoleLandingPage>> roleLandingPage() {
		Set<RoleLandingPage> setCSVOutPut = memberEngagementService.getRoleLandingPage();
		return new ResponseEntity<Set<RoleLandingPage>>(setCSVOutPut, HttpStatus.OK);
	}
}
