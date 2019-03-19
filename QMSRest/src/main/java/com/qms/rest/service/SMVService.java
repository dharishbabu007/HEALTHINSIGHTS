package com.qms.rest.service;

import java.util.Set;

import com.qms.rest.model.LhcMemberView;
import com.qms.rest.model.LhrMemberListView;
import com.qms.rest.model.SMVMemberDetails;
import com.qms.rest.model.SMVMemberPayerClustering;
import com.qms.rest.model.SmvMemberClinical;

public interface SMVService {

	Set<SMVMemberDetails> getSMVMemberDetails(String memberId);

	Set<SmvMemberClinical> getSmvMemberClinical(String memberId);

	Set<SMVMemberPayerClustering> getSMVMemberPayerClustering(String memberId);
	
	Set<LhcMemberView> getLhcMemberViewList();
	
	Set<LhrMemberListView> getLhrMemberListView();
	
}
