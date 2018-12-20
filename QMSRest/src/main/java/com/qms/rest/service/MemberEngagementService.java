package com.qms.rest.service;

import java.util.Set;

import com.qms.rest.model.ClusterAnalysis;
import com.qms.rest.model.ClusterData;
import com.qms.rest.model.ClusterPersona;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;

public interface MemberEngagementService {
	
	Set<ModelSummary> getCSVModelSummary();
	Set<ConfusionMatric> getCSVConfusionMatric();	
	ModelScore getCSVModelScore();
	Set<ClusterAnalysis> getCSVClusterAnalysis();
	
	RestResult updateClusteringPersona (ClusterPersona clusteringPersona);
	ClusterData getClusteringData (int clusterId);
	
	Set<LHEOutput> getLHEModelOutPut();
	Set<ModelSummary> getLHEModelSummary();	
	ModelMetric getLHEModelMetric();	
	String[][] getLHEReasonNotEnrollStatics();
	
	RestResult createLHEInputFile();
}
