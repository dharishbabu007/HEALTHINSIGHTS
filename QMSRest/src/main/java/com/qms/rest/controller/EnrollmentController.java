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

import com.qms.rest.model.EnrollmentFileOutput;
import com.qms.rest.model.FactGoalRecommendations;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.PersonaMemberListView;
import com.qms.rest.model.RefModel;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RewardsFileOutput;
import com.qms.rest.service.EnrollmentService;

@RestController
@RequestMapping("/enrollment")
@CrossOrigin
public class EnrollmentController {
	
	@Autowired
	EnrollmentService enrollmentService;
	
	@RequestMapping(value = "/get_enrollment_file_output/{criteria}", method = RequestMethod.GET)
	public ResponseEntity<List<EnrollmentFileOutput>> getEnrollmentFileOutput(@PathVariable("criteria") String criteria) {
		System.out.println("Fetching personaMemberList ");
		List<EnrollmentFileOutput> setCSVOutPut = enrollmentService.getEnrollmentFileOutput(criteria);
		return new ResponseEntity<List<EnrollmentFileOutput>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/update_enrollment_file_output/{tab}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateClusteringPersona(@PathVariable("tab") String tab, 
			@RequestBody List<EnrollmentFileOutput> enrollmentFileOutputList, 
			UriComponentsBuilder ucBuilder) {
		RestResult restResult = enrollmentService.updateEnrollmentFileOutput(enrollmentFileOutputList, tab);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/get_objectives_list/{title}", method = RequestMethod.GET)
	public ResponseEntity <Objectives> getObjectivesByTitle(@PathVariable("title") String title) {
		Objectives workList = enrollmentService.getObjectivesByTitle(title);
		if (workList==null) {
			return new ResponseEntity<Objectives>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Objectives>(workList,HttpStatus.OK);
	}
	
	
	@RequestMapping(value = "/dropdown_list/{tableName}/{columnName}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> listDropDownValues(@PathVariable("tableName") String tableName, 
			@PathVariable("columnName") String columnName) {
		Set<String> dataList = enrollmentService.getDataByTableAndColumn(tableName, columnName);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getCearGapList/{mid}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getCearGapList(@PathVariable("MEMBER_ID") String mid ){ 
		
		Set<String> dataList = enrollmentService.getCearGapList(mid);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/Fact_Goal_Recommendations_create", method = RequestMethod.POST)
	public ResponseEntity<RestResult> createPatCreater(@RequestBody FactGoalRecommendations factGoalRecommendations, UriComponentsBuilder ucBuilder) {
	
		System.out.println(" Creating Fact Goal Recommendations with status --> " + factGoalRecommendations.getRecommendationId());
		RestResult restResult = enrollmentService.insertFactGoalRecommendationsCreator (factGoalRecommendations);

		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path("/api/factGoalRecommendations/{id}").buildAndExpand(10).toUri());
		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.CREATED);
	}	
	
	@RequestMapping(value = "/getCareGapList/{mid}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getCareGapList(@PathVariable("mid") String mid ){ 
		
		Set<String> dataList = enrollmentService.getcareGapMeasurelist(mid);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_Persona_Member_list/{mid}", method = RequestMethod.GET)
	public ResponseEntity<PersonaMemberListView> getPersonaMemberList(@PathVariable("mid") String mid ) {
		PersonaMemberListView workList = enrollmentService.getPersonaMemberList(mid);
		if (workList==null) {
			return new ResponseEntity<PersonaMemberListView>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<PersonaMemberListView>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/update_rewards/{memberId}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateRewards(@PathVariable("memberId") String memberId, 
			@RequestBody List<String> rewardsList, 
			UriComponentsBuilder ucBuilder) {
		RestResult restResult = enrollmentService.updateRewards(memberId, rewardsList);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}
	
	@RequestMapping(value = "/get_all_rewards/{questionId}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getAllRewards(@PathVariable("questionId") String questionId){ 
		
		Set<String> dataList = enrollmentService.getAllRewards(questionId);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_Rewards_File_Output_list/{mid}", method = RequestMethod.GET)
	public ResponseEntity<RewardsFileOutput> getRewardsFileOutputList(@PathVariable("mid") String mid) {
		System.out.println("==============/get_Rewards_File_Output_list/{mid}========cont==========");
		RewardsFileOutput workList = enrollmentService.getRewardsFileOutputList(mid);
		if (workList == null) {
			return new ResponseEntity<RewardsFileOutput>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<RewardsFileOutput>(workList, HttpStatus.OK);
	}

	@RequestMapping(value = "/rewards_File_Output_update", method = RequestMethod.POST)
	public ResponseEntity<RestResult> updateRewardsFileOutput(@RequestBody RewardsFileOutput rewardsFileOutput,
			UriComponentsBuilder ucBuilder) {
		System.out.println(" Update Fact Rewards File Output with status --> " + rewardsFileOutput.getRewardId());
		RestResult restResult = enrollmentService.insertRewardsFileOutput(rewardsFileOutput);

		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path("/api/rewards_File_Output/{id}").buildAndExpand(10).toUri());
		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.CREATED);
	}	
	
}
