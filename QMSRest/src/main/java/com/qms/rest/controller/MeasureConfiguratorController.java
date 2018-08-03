package com.qms.rest.controller;

import java.util.List;
import java.util.Set;

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
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.MeasureConfig;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.TableData;
import com.qms.rest.service.MeasureConfigService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/measure_configurator")
@CrossOrigin
public class MeasureConfiguratorController {
	
	@Autowired
	MeasureConfigService measureConfigService;	
	
	public static final Logger logger = LoggerFactory.getLogger(MeasureConfiguratorController.class);
	
	@RequestMapping(value = "/config_data", method = RequestMethod.GET)
	public ResponseEntity<Set<TableData>> getMeasureConfigData() {
		Set<TableData> data = measureConfigService.getMeasureConfigData();
		if (data.isEmpty()) {
			return new ResponseEntity(HttpStatus.NO_CONTENT);			
		}		
		return new ResponseEntity<Set<TableData>>(data, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/{measure_id}/{category}", method = RequestMethod.POST)
	public ResponseEntity<?> createMeasureConfig(@PathVariable("measure_id") String measureId,
			@PathVariable("category") String category, 
			@RequestBody List<MeasureConfig> measureConfigList, 
			UriComponentsBuilder ucBuilder) {
		logger.info("Creating MeasureConfigs : {}", measureConfigList);
		System.out.println(" Creating MeasureConfigs --> " + measureConfigList);
		if(measureConfigList == null || measureConfigList.isEmpty()) {
			return new ResponseEntity(new CustomErrorType(" Measure configuration list is null. "), HttpStatus.BAD_REQUEST);			
		}
		
		System.out.println(" Adding config list ... for " + category);
		for (MeasureConfig measureConfig : measureConfigList) {
			measureConfig.setMeasureId(measureId);
			measureConfig.setCategory(category);
			System.out.println(measureConfig.getOperator() + " :: " + measureConfig.getTechnicalExpression() + " :: " +  
					measureConfig.getBusinessExpression());
		}
		
		RestResult restResult = measureConfigService.insertMeasureConfig(measureConfigList, category);
		return new ResponseEntity<RestResult>(restResult, HttpStatus.CREATED);
	}	
	
	
	@RequestMapping(value = "/{measure_id}/{category}", method = RequestMethod.PUT)
	public ResponseEntity<RestResult> updateMeasureConfig(@PathVariable("measure_id") String measureId,
			@PathVariable("category") String category,
			@RequestBody List<MeasureConfig> measureConfigList) {
		logger.info("Updating MeasureCreator with id {}", measureId);
		
		List<MeasureConfig> existMeasureConfigs = measureConfigService.getMeasureConfigById(measureId, category);
		System.out.println(" Existing Measure Configs in DB --> " + existMeasureConfigs);
		RestResult restResult = null;
		if(existMeasureConfigs == null || existMeasureConfigs.isEmpty()) {
			System.out.println(" Inserting measure configs ...");
			restResult = measureConfigService.insertMeasureConfig(measureConfigList, category);
		} else {
			System.out.println(" Updating measure configs ...");
			restResult = measureConfigService.updateMeasureConfig(measureConfigList, category);
		}
		
		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/{measure_id}/{category}", method = RequestMethod.GET)
	public ResponseEntity<List<MeasureConfig>> getMeasureConfig(@PathVariable("measure_id") String measureId, 
			@PathVariable("category") String category) {
		logger.info("Updating MeasureCreator with id {}", measureId);
		
		List<MeasureConfig> existMeasureConfigs = measureConfigService.getMeasureConfigById(measureId, category);
		if (existMeasureConfigs.isEmpty()) {
			return new ResponseEntity<List<MeasureConfig>>(HttpStatus.NO_CONTENT);			
		}		
		
		return new ResponseEntity<List<MeasureConfig>>(existMeasureConfigs, HttpStatus.OK);
	}	

}
