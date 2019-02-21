package com.qms.rest.service;

import java.util.List;
import java.util.Set;

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

public interface EnrollmentService {
	List<EnrollmentFileOutput> getEnrollmentFileOutput (String criteria);
	RestResult updateEnrollmentFileOutput (List<EnrollmentFileOutput> enrollmentFileOutputList, String flag);
	
	public Objectives getObjectivesByTitle(String title);
	
	public Set<String> getCareGapListByMemberId(String memberId);
	RestResult insertFactGoalRecommendationsCreator(FactGoalRecommendations factGoalRecommendations);
	
	public PersonaMemberListView getPersonaMemberList(String mid);
	
	public Set<String> getDataByTableAndColumn(String tableName, String columnName);
	RestResult updateRewards(String memberId, List<String> rewards);
	public Set<String> getAllRewards(String questionId);
	
	public RewardsFileOutput getRewardsFileOutputList(String mid);
	public RestResult insertRewardsFileOutput(RewardsFileOutput rewardsFileOutput);	
	public RestResult insertRewardsRecommendations(RewardsRecommendations rewardsRecommendations);		
	public Set<PersonaMemberListView> filterPersonaMemberList(String filterType, String filterValue);
	
//	public Set<GoalRecommendation> getGoalRecommendationsMemberList();
//	public Set<GoalSet> getGoalsSetMemberList();
	public Set<GoalRecommendationSet> getGoalRecommendationsSetMemberList();	
	
	public Set<RewardRecommendationSet> getRewardRecommendationsSetMemberList();
}
