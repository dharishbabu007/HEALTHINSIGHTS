package com.qms.rest.controller;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.QMSFile;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.ImportExportService;

@RestController
@RequestMapping("/qms_file")
@CrossOrigin
public class ImportExportController {

	public static final Logger logger = LoggerFactory.getLogger(ImportExportController.class);
	
	@Autowired
	ImportExportService importExportService;
	
	@RequestMapping(value = "/import", method = RequestMethod.POST)
	public ResponseEntity<RestResult> importFile(@RequestBody QMSFile file, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = importExportService.importFile(file.getFile());
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Access-Control-Allow-Origin", "*");		
		headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");				
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.INTERNAL_SERVER_ERROR);
	}
	
	@RequestMapping(value = "/export", method = RequestMethod.POST)
	public ResponseEntity<RestResult> exportFile(@RequestBody QMSFile file, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = importExportService.exportFile(file.getFile());
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	@RequestMapping(value = "/run_r", method = RequestMethod.POST)
	public ResponseEntity<RestResult> runRFile(@RequestBody QMSFile file, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = importExportService.runRFile(file.getFile());
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}

		return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	
	@RequestMapping(value = "/csv_output", method = RequestMethod.GET)
	public ResponseEntity<Set<CSVOutPut>> getOutputCSVData() {
		System.out.println("Fetching Output.csv data ");
		Set<CSVOutPut> setCSVOutPut = importExportService.getCSVOutPut();
		return new ResponseEntity<Set<CSVOutPut>>(setCSVOutPut, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/csv_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getModelSummaryCSVData() {
		System.out.println("Fetching ModelSummary.csv data ");
		Set<ModelSummary> setCSVOutPut = importExportService.getCSVModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	
}
