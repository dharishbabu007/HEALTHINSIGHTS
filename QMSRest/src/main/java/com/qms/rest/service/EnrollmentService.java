package com.qms.rest.service;

import java.util.List;
import java.util.Set;

import com.qms.rest.model.EnrollmentFileOutput;
import com.qms.rest.model.FactGoalRecommendations;
import com.qms.rest.model.Objectives;
import com.qms.rest.model.PersonaMemberListView;
import com.qms.rest.model.RefModel;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RewardsFileOutput;

public interface EnrollmentService {
	List<EnrollmentFileOutput> getEnrollmentFileOutput (String criteria);
	RestResult updateEnrollmentFileOutput (List<EnrollmentFileOutput> enrollmentFileOutputList, String flag);
	

	public Objectives getObjectivesByTitle(String title);
	
	public Set<String> getCearGapList(String mid);
	RestResult insertFactGoalRecommendationsCreator(FactGoalRecommendations factGoalRecommendations);
	
	public Set<String> getcareGapMeasurelist(String mid);	
	public PersonaMemberListView getPersonaMemberList(String mid);
	
	public Set<String> getDataByTableAndColumn(String tableName, String columnName);
	RestResult updateRewards(String memberId, List<String> rewards);
	public Set<String> getAllRewards(String questionId);
	
	public RewardsFileOutput getRewardsFileOutputList(String mid);
	public RestResult insertRewardsFileOutput(RewardsFileOutput rewardsFileOutput);	
}
