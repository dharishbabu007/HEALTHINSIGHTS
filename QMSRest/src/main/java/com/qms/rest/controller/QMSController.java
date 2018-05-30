package com.qms.rest.controller;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.DimPatient;
import com.qms.rest.model.Measure;
import com.qms.rest.model.MeasureCreator;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.service.PatientService;
import com.qms.rest.service.QMSService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/qms")
public class QMSController {
	
	public static final Logger logger = LoggerFactory.getLogger(QMSController.class);
	
	@Autowired
	QMSService qmsService; 	
	
	@Autowired
	PatientService patientService;	
	

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
	public ResponseEntity<?> measureLibraryById(@PathVariable("id") int id) {
		logger.info("Fetching Measure with id {}", id);
		Measure measure = qmsService.getMeasureLibraryById(id);
		if (measure == null) {
			logger.error("Measure with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("Measure with id " + id 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<Measure>(measure, HttpStatus.OK);
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
	
	@RequestMapping(value = "/dropdown_namevalue_list/{tableName}/{columnValue}/{columnName}", method = RequestMethod.GET)
	public ResponseEntity<Set<NameValue>> dropDownNameValueList(@PathVariable("tableName") String tableName, 
			@PathVariable("columnValue") String columnValue, @PathVariable("columnName") String columnName) {
		Set<NameValue> dataList = qmsService.getMeasureDropDownNameValueList(tableName, columnValue, columnName);
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
	public ResponseEntity<?> getMeasureCreator(@PathVariable("id") String id) {
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
	public ResponseEntity<RestResult> updateMeasureCreator(@PathVariable("id") String id, @RequestBody MeasureCreator measureCreator) {
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
	public ResponseEntity<RestResult> updateMeasureCreatorStatus(@PathVariable("id") String id, 
			@PathVariable("status") String status) {
		System.out.println("REST Update Measure WorkList Status for id : " + id + " with status : " + status);
		MeasureCreator currentMeasureCreator = qmsService.findMeasureCreatorById(id);
		if (currentMeasureCreator == null) {
			logger.error("Measurecreator with id {} not found.", id);
			return new ResponseEntity(new CustomErrorType("Measurecreator with id " + id 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		
		RestResult restResult = qmsService.updateMeasureWorkListStatus(id, status);
		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
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

	@RequestMapping(value = "/user/{loginId}/{password}", method = RequestMethod.GET)
	public ResponseEntity<?> getUserDetails(@PathVariable("loginId") String loginId, @PathVariable("password") String password) {
		logger.info("Fetching user details for loginId {}", loginId);
		System.out.println("Fetching user details for loginId " + loginId);
		User user = patientService.getUserInfo(loginId, password);
		if (user == null) {
			logger.error("User details with loginId {} not found.", loginId);
			return new ResponseEntity(new CustomErrorType("User details with loginId " + loginId 
					+ " not found"), HttpStatus.NOT_FOUND);
		}
		System.out.println("Returned user name for loginId " + loginId + " : " + user.getName());
		return new ResponseEntity<User>(user, HttpStatus.OK);
	}	
	
}
