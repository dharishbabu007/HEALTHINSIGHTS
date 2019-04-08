package com.qms.rest.controller;

import java.util.List;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.qms.rest.exception.QMSException;
import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.DimMemberGapListSearch;
import com.qms.rest.model.DimMemeberList;
import com.qms.rest.model.MemberCareGapsList;
import com.qms.rest.model.QMSMemberReq;
import com.qms.rest.service.MemberGapListService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/memberGapList")
@CrossOrigin
public class MemberGapListController {

	public static final Logger logger = LoggerFactory.getLogger(MemberGapListController.class);

	@Autowired
	private MemberGapListService memberGapList;
	
		
	@RequestMapping(value = "/member/{mid}", method = RequestMethod.GET)
	public ResponseEntity<DimMemeberList> getMemberGapList(@PathVariable("mid") String mid) {
		System.out.println("Fetching dimMemberGaps with mid {}"+ mid);
		logger.info("Fetching dimMemberGaps with mid {}", mid);
		DimMemeberList dimMemberGaps = memberGapList.findMembergapListByMid(mid);
		if (dimMemberGaps == null) {
			logger.error("MeasureCreator with id {} not found.", mid);
			return new ResponseEntity(new CustomErrorType("dimMemberGaps with mid " + mid 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity((DimMemeberList) dimMemberGaps, HttpStatus.OK);
	}

	@RequestMapping(value = "/memberQMS", method = RequestMethod.POST, consumes=MediaType.APPLICATION_JSON)
	public ResponseEntity<QMSMemberReq> editMemberGapQMSList(@RequestBody QMSMemberReq qMSMemberReq) {
		logger.info("updating data with dimMemberGaps with mid {}", qMSMemberReq);
		try {
			return new ResponseEntity((QMSMemberReq) memberGapList.editMembergapListByQMS(qMSMemberReq), HttpStatus.OK);
		} catch (QMSException e) {
			logger.error("editMemberGapQMSList insertion process failed."+ e.getMessage(), qMSMemberReq.getCareGapsId());
			return new ResponseEntity(new CustomErrorType("editMemberGapQMSList insertion process failed."), HttpStatus.BAD_REQUEST);
		}
		
	}
	
	@RequestMapping(value = "/findAllMembers", method = RequestMethod.GET )
	public ResponseEntity<List<MemberCareGapsList>> findAllMembersList() {
		logger.info("Fetching dimMemberGaps with mid {}");
		List<MemberCareGapsList>  memberCareGapsList = memberGapList.findAllMembersList();
		//List<MemberCareGapsList>  memberCareGapsList = memberGapList.findAllMembersListFromHive();		
		if (memberCareGapsList == null) {
			logger.error("Not able to find the Members .", memberCareGapsList);
			return (ResponseEntity<List<MemberCareGapsList>>) new ResponseEntity(new CustomErrorType("dimMemberGaps with mid " + memberCareGapsList 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return (ResponseEntity<List<MemberCareGapsList>>) new ResponseEntity((List<MemberCareGapsList>) memberCareGapsList, HttpStatus.OK);
	}
	 
	@RequestMapping(value = "/findAllMembersHive", method = RequestMethod.GET)
	public ResponseEntity<List<MemberCareGapsList>> findAllMembersHive() {
		logger.info("Fetching dimMemberGaps with mid {}");
		List<MemberCareGapsList>  memberCareGapsList = memberGapList.findAllMembersListFromHive();
		if (memberCareGapsList == null || memberCareGapsList.isEmpty()) {
			return new ResponseEntity<List<MemberCareGapsList>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<MemberCareGapsList>>(memberCareGapsList, HttpStatus.OK);
	}	 
	 
	@RequestMapping(value = "/getHomePageCareGapsList", method = RequestMethod.GET )
	public ResponseEntity<List<MemberCareGapsList>> getHomePageCareGapsList() {
		List<MemberCareGapsList>  memberCareGapsList = memberGapList.getHomePageCareGapsList();
		if (memberCareGapsList == null || memberCareGapsList.isEmpty()) {
			return new ResponseEntity<List<MemberCareGapsList>>(HttpStatus.NO_CONTENT);
		}
		return new ResponseEntity<List<MemberCareGapsList>>(memberCareGapsList, HttpStatus.OK);
	}	 
	
	@RequestMapping(value = "/searchMember/{search}", method = RequestMethod.GET)
	public ResponseEntity<List<DimMemberGapListSearch>> getSearchMemberGapList(@PathVariable("search") String search) {
			System.out.println("Fetching dimMemberGaps with mid {}"+ search);
			logger.info("Fetching dimMemberGaps with mid {}", search);
			search = search.substring(0, 1).toUpperCase()+search.substring(1);
			List<DimMemberGapListSearch> dimMemberGapListSearch = memberGapList.findSearchMembergapList(search);
			if (dimMemberGapListSearch == null) {
				logger.error("MeasureCreator with id {} not found.", search);
				return new ResponseEntity(new CustomErrorType("dimMemberGapListSearch  " + search 
						+ " not found"), HttpStatus.NOT_FOUND);
			}
			return new ResponseEntity<List<DimMemberGapListSearch>>((List<DimMemberGapListSearch>) dimMemberGapListSearch, HttpStatus.OK);
	}
	 
}
