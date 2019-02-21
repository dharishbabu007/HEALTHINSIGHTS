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
import com.qms.rest.model.GoalRecommendation;
import com.qms.rest.model.GoalRecommendationSet;
import com.qms.rest.model.GoalSet;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.PersonaMemberListView;
import com.qms.rest.model.RefModel;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RewardRecommendationSet;
import com.qms.rest.model.RewardsFileOutput;
import com.qms.rest.model.RewardsRecommendations;
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
	
	@RequestMapping(value = "/get_caregap_list_by_memberid/{memberId}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getCareGapListByMemberId(@PathVariable("memberId") String memberId ){ 
		
		Set<String> dataList = enrollmentService.getCareGapListByMemberId(memberId);
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
	
	@RequestMapping(value = "/insert_Rewards_Recommendations", method = RequestMethod.POST)
	public ResponseEntity<RestResult> insertRewardsRecommendations(@RequestBody RewardsRecommendations rewardsRecommendations,
			UriComponentsBuilder ucBuilder) {
		System.out.println(" Insert Rewards Recommendations --> " + rewardsRecommendations.getRewardRecId());
		RestResult restResult = enrollmentService.insertRewardsRecommendations(rewardsRecommendations);

		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path("/api/insert_Rewards_Recommendations/{id}").buildAndExpand(10).toUri());
		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.CREATED);
	}	
	
	@RequestMapping(value = "/filter_Persona_Member_list/{filterType}/{filterValue}", method = RequestMethod.GET)
	public ResponseEntity<Set<PersonaMemberListView>> filterPersonaMemberList(@PathVariable("filterType") String filterType, 
			@PathVariable("filterValue") String filterValue ) {
		Set<PersonaMemberListView> workList = enrollmentService.filterPersonaMemberList(filterType, filterValue);
		if (workList==null) {
			return new ResponseEntity<Set<PersonaMemberListView>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<PersonaMemberListView>>(workList, HttpStatus.OK);
	}
	
//	@RequestMapping(value = "/getGoalRecommendationsMemberList", method = RequestMethod.GET)
//	public ResponseEntity<Set<GoalRecommendation>> getGoalRecommendationsMemberList() {
//		Set<GoalRecommendation> workList = enrollmentService.getGoalRecommendationsMemberList();
//		if (workList==null) {
//			return new ResponseEntity<Set<GoalRecommendation>>(HttpStatus.NO_CONTENT);
//		}
//		return new ResponseEntity<Set<GoalRecommendation>>(workList, HttpStatus.OK);
//	}
//	
//	@RequestMapping(value = "/getGoalsSetMemberList", method = RequestMethod.GET)
//	public ResponseEntity<Set<GoalSet>> getGoalsSetMemberList() {
//		Set<GoalSet> workList = enrollmentService.getGoalsSetMemberList();
//		if (workList==null) {
//			return new ResponseEntity<Set<GoalSet>>(HttpStatus.NO_CONTENT);
//		}
//		return new ResponseEntity<Set<GoalSet>>(workList, HttpStatus.OK);
//	}
	
	@RequestMapping(value = "/getGoalRecommendationsSetMemberList", method = RequestMethod.GET)
	public ResponseEntity<Set<GoalRecommendationSet>> getGoalRecommendationsSetMemberList() {
		Set<GoalRecommendationSet> workList = enrollmentService.getGoalRecommendationsSetMemberList();
		if (workList==null) {
			return new ResponseEntity<Set<GoalRecommendationSet>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<GoalRecommendationSet>>(workList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getRewardRecommendationsSetMemberList", method = RequestMethod.GET)
	public ResponseEntity<Set<RewardRecommendationSet>> getRewardRecommendationsSetMemberList() {
		Set<RewardRecommendationSet> workList = enrollmentService.getRewardRecommendationsSetMemberList();
		if (workList==null) {
			return new ResponseEntity<Set<RewardRecommendationSet>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<Set<RewardRecommendationSet>>(workList, HttpStatus.OK);
	}	
	
}
