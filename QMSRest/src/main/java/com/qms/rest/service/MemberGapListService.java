package com.qms.rest.service;

import java.util.List;

import com.qms.rest.exception.QmsInsertionException;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.DimMemeberList;
import com.qms.rest.model.MemberCareGapsList;
import com.qms.rest.model.QMSMemberReq;

public interface MemberGapListService {
	
		DimMemeberList findMembergapListByMid(String mid);

		QMSMemberReq editMembergapListByQMS(QMSMemberReq qMSMemberReq) throws QmsInsertionException;

		List<MemberCareGapsList> findAllMembersList();

		List<DimMemberGapListSearch> findSearchMembergapList(String search);   


}
