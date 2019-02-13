package com.qms.rest.service;

import java.util.List;
import java.util.Set;

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
import com.qms.rest.model.PersonaClusterFeatures;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.RoleLandingPage;

public interface MemberEngagementService {
	
	Set<ModelSummary> getCSVModelSummary();
	Set<ConfusionMatric> getCSVConfusionMatric();	
	ModelScore getCSVModelScore();
	String[][] getCSVClusterAnalysis();
	
	RestResult updateClusteringPersona (PersonaDefine clusteringPersona);
	ClusterData getClusteringData (String clusterId);
	GraphData getPersonaClusterFeaturesGraphData(String clusterId, String attributeName);
	Set<PersonaClusterFeatures> getPersonaClusterFeatures(String clusterId);
	Set<String> getPersonaClusterNames();
	Set<RoleLandingPage> getRoleLandingPage();
	
	///////////LHE////////////////////
	Set<LHEOutput> getLHEModelOutPut();
	Set<ModelSummary> getLHEModelSummary();	
	ModelMetric getLHEModelMetric();	
	String[][] getLHEReasonNotEnrollStatics();	
	RestResult createLHEInputFile();	
	
	///////////Persona///////////////////
	RestResult createPersonaInputFile();	
	Set<PersonaMember> personaMemberList(String clusterId);
	
	///////////LHC////////////////////
	RestResult createLHCInputFile();	
	Set<LHCMember> lhcMemberList();
	Set<ModelSummary> getLHCModelSummary();	
	ModelMetric getLHCModelMetric();	
}
