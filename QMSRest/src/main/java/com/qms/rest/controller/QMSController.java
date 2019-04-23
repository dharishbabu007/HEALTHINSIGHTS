package com.qms.rest.controller;

import java.util.List;
import java.util.Set;

import com.qms.rest.model.*;
import com.qms.rest.service.MemberService;
import com.qms.rest.service.ProgramService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.service.PatientService;
import com.qms.rest.service.QMSService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/qms")
@CrossOrigin
public class QMSController {
	
	public static final Logger logger = LoggerFactory.getLogger(QMSController.class);
	
	@Autowired
	QMSService qmsService; 	
	
	@Autowired
	PatientService patientService;	
	
	@Autowired
	private ProgramService programService;

	@Autowired
	private MemberService memberService;

	//@Autowired
	//private ObjectMapper objectMapper;
	

	@RequestMapping(value = "/measure_list/{type}/{value}", method = RequestMethod.GET)
	public ResponseEntity<Set<MeasureCreator>> listMeasures(@PathVariable("type") String type, 
			@PathVariable("value") String value) {
		Set<MeasureCreator> measures = qmsService.getMeasureLibrary(type, value);
		if (measures.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<MeasureCreator>>(measures, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/measure_list/{id}", method = RequestMethod.GET)
	public ResponseEntity<MeasureCreator> measureLibraryById(@PathVariable("id") int id) {
		logger.info("Fetching Measure with id {}", id);
		MeasureCreator measure = qmsService.getMeasureLibraryById(id);
		if (measure == null) {
			logger.error("Measure with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("Measure with id " + id 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<MeasureCreator>(measure, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/dropdown_list/{tableName}/{columnName}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> listDropDownValues(@PathVariable("tableName") String tableName, 
			@PathVariable("columnName") String columnName) {
		Set<String> dataList = qmsService.getMeasureDropDownList(tableName, columnName);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/dropdown_namevalue_list/{tableName}/{idColumn}/{nameColumn}", method = RequestMethod.GET)
	public ResponseEntity<Set<NameValue>> dropDownNameValueList(@PathVariable("tableName") String tableName, 
			@PathVariable("idColumn") String idColumn, @PathVariable("nameColumn") String nameColumn) {
		Set<NameValue> dataList = qmsService.getMeasureDropDownNameValueList(tableName, idColumn, nameColumn);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<NameValue>>(dataList, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/qmshome_dropdown_list/{tableName}/{columnName}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> qmsHomeDropDownValues(@PathVariable("tableName") String tableName, 
			@PathVariable("columnName") String columnName) {
		Set<String> dataList = qmsService.getQMSHomeDropDownList(tableName, columnName);
		if (dataList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(dataList, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/work_list/{id}", method = RequestMethod.GET)
	public ResponseEntity<?> getMeasureCreator(@PathVariable("id") int id) {
		logger.info("Fetching MeasureCreator with id {}", id);
		MeasureCreator measureCreator = qmsService.findMeasureCreatorById(id);
		if (measureCreator == null) {
			logger.error("MeasureCreator with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("MeasureCreator with id " + id 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<MeasureCreator>(measureCreator, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/work_list/", method = RequestMethod.POST)
	public ResponseEntity<RestResult> createMeasureCreater(@RequestBody MeasureCreator measureCreator, UriComponentsBuilder ucBuilder) {
		logger.info("Creating MeasureCreator : {}", measureCreator);
		System.out.println(" Creating MeasureCreator with status --> " + measureCreator.getStatus());
		RestResult restResult = qmsService.insertMeasureCreator(measureCreator);

		HttpHeaders headers = new HttpHeaders();
		headers.setLocation(ucBuilder.path("/api/MeasureCreator/{id}").buildAndExpand(10).toUri());
		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.CREATED);
	}	

	@RequestMapping(value = "/work_list/{id}", method = RequestMethod.PUT)
	public ResponseEntity<RestResult> updateMeasureCreator(@PathVariable("id") int id, @RequestBody MeasureCreator measureCreator) {
		logger.info("Updating MeasureCreator with id {}", id);
		measureCreator.setId(id);
		MeasureCreator currentMeasureCreator = qmsService.findMeasureCreatorById(id);
		System.out.println(" Selected MeasureCreator --> " + currentMeasureCreator);
		System.out.println(" Storing the measure data for id --> " + measureCreator.getId() + ". Name --> " + measureCreator.getName() 
							+ " status --> " + measureCreator.getStatus());
		RestResult restResult = null;
		if (currentMeasureCreator == null) {
			restResult = qmsService.insertMeasureCreator(measureCreator);
		} else {
			restResult = qmsService.updateMeasureCreator(measureCreator);
		}
		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/work_list/", method = RequestMethod.GET)
	public ResponseEntity<Set<MeasureCreator>> listWorklist() {
		Set<MeasureCreator> workList = qmsService.getAllWorkList();
		if (workList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<MeasureCreator>>(workList, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/work_list/status/{id}/{status}", method = RequestMethod.PUT)
	public ResponseEntity<RestResult> updateMeasureCreatorStatus(@PathVariable("id") int id, 
			@PathVariable("status") String status,
			@RequestBody Param param) {
		System.out.println("REST Update Measure WorkList Status for id : " + id + " with status : " + status);
		MeasureCreator currentMeasureCreator = qmsService.findMeasureCreatorById(id);
		if (currentMeasureCreator == null) {
			logger.error("Measurecreator with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("Measurecreator with id " + id 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		
		RestResult restResult = qmsService.updateMeasureWorkListStatus(id, status, param);
		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/get_category_by_program_id/{programId}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getCategoryByProgramId(@PathVariable("programId") String programId) {
		Set<String> workList = qmsService.getCategoryByProgramId(programId);
		if (workList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(workList, HttpStatus.OK);
	}		
	
	@RequestMapping(value = "/spv/{programType}/{id}", method = RequestMethod.GET)
	public ResponseEntity<?> getSPVDetails(@PathVariable("programType") String programType, @PathVariable("id") String id) {
		logger.info("Fetching SPV details with id {}", id);
		DimPatient dimPatient = null;
		if(programType.equalsIgnoreCase("mips"))
			dimPatient = patientService.getPatientById(id);
		else if(programType.equalsIgnoreCase("hedis"))
			dimPatient = patientService.getMemberById(id);
		
		if (dimPatient == null) {
			logger.error("SPV details with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("SPV details with id " + id + " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<DimPatient>(dimPatient, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/spv/hedis_member_list", method = RequestMethod.GET)
	public ResponseEntity<?> getMemberDetails() {
		logger.info("Fetching MemberDetails");
		Set<MemberDetail> memberDetails = null;
		memberDetails = patientService.getMemberDetails();
		
		if (memberDetails == null) {
			logger.error(" Unable to fetch MemberDetails details");
			return new ResponseEntity(new CustomErrorType("Unable to fetch MemberDetails details"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Set<MemberDetail>>(memberDetails, HttpStatus.OK);
	}		

//	@RequestMapping(value = "/createProgram", method = RequestMethod.POST)
//	@ResponseBody
//	public ResponseEntity<RestResult> createProgram(@RequestBody Program program) {
//		String response = "Program created successfully for ProgramName : "+program.getProgramName();
//		logger.info("About to create program :  " + program);
//		programService.createProgram(program);
//		
//		RestResult restResult = RestResult.getSucessRestResult(response);		
//		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
//	}

	@RequestMapping(value = "/members/{dimMemberId}", method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<?> findDimMember(@PathVariable("dimMemberId") String dimMemberId) {
		logger.info("About to get dim member for id :  " + dimMemberId);
		List<DimMember> dimMemberList = memberService.findDimMembers(dimMemberId);
		HttpHeaders headers = new HttpHeaders();
		return new ResponseEntity<Object>(dimMemberList, headers, HttpStatus.OK);
	}
	
//	@RequestMapping(value = "/program/{programId}", method = RequestMethod.GET)
//	@ResponseBody
//	public ResponseEntity<?> getProgramById(@PathVariable("programId") int programId) {
//
//		QualityProgramUI qualityProgram = programService.getProgramById(programId);
//		if (qualityProgram == null) {
//			logger.error(" Unable to fetch program details");
//			return new ResponseEntity(new CustomErrorType("Program not found."), HttpStatus.NOT_FOUND);
//		}
//		return new ResponseEntity<QualityProgramUI>(qualityProgram, HttpStatus.OK);
//	}
	
	@RequestMapping(value = "/refMrss_list", method = RequestMethod.GET)
	public ResponseEntity<Set<RefMrss>> refMrssList(){
		Set<RefMrss> workList = qmsService.getRefMrssList();
		if (workList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<RefMrss>>(workList, HttpStatus.OK);
	}	
	
	
	@RequestMapping(value = "/refMrss_Sample_list", method = RequestMethod.GET)
	public ResponseEntity<Set<RefMrssSample>> refMrssSampleList() {
		Set<RefMrssSample> workList = qmsService.getRefMrssSaimpleList();
		if (workList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<RefMrssSample>>(workList, HttpStatus.OK);
	}
	

	@RequestMapping(value = "/getProductPlanByLobId/{lobId}", method = RequestMethod.GET)
	public ResponseEntity<Set<String>> getProductPlanByLobId(@PathVariable("lobId") String lobId) {
		Set<String> workList = qmsService.getProductPlanByLobId(lobId);
		if (workList.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<Set<String>>(workList, HttpStatus.OK);
	}	
	
}
