package com.qms.rest.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.CSVOutPut1;
import com.qms.rest.model.ComplianceOutPut;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.FileUpload;
import com.qms.rest.model.LHEOutput;
import com.qms.rest.model.ModelMetric;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.QMSFile;
import com.qms.rest.model.RestResult;
import com.qms.rest.model.User;
import com.qms.rest.service.ImportExportService;
import com.qms.rest.util.QMSConstants;

@RestController
@RequestMapping("/qms_file")
@CrossOrigin
public class ImportExportController {

	public static final Logger logger = LoggerFactory.getLogger(ImportExportController.class);
	
	@Autowired
	ImportExportService importExportService;
	
	@Autowired 
	private HttpSession httpSession;	
	
	@RequestMapping(value = "/import/{model}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> importFile(@RequestParam("file") MultipartFile uploadfile, 
			@PathVariable("model") String model) {
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Access-Control-Allow-Origin", "*");		
		headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
		
		List<String> modelList = Arrays.asList(new String[]{"noshow", "lhe", "lhc", "persona", "nc"});
		if(!modelList.contains(model)) {
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("Invalid model. Please select valid model. "), headers, 
            		HttpStatus.BAD_REQUEST);			
		}		
		
		if (uploadfile.isEmpty()) {            
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("File is empty. Please select a valid file!"), headers, 
            		HttpStatus.BAD_REQUEST);
        }
		
		String extension = FilenameUtils.getExtension(uploadfile.getOriginalFilename());
		if(!extension.equalsIgnoreCase("CSV")) {
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("Invalid file. File type should be CSV. "), headers, 
            		HttpStatus.BAD_REQUEST);			
		}
		//storing file meta data 
		User user = (User) httpSession.getAttribute(QMSConstants.SESSION_USER_OBJ);
		FileUpload fileUpload = new FileUpload();
		fileUpload.setFileName(uploadfile.getOriginalFilename());
		fileUpload.setDateTime(new java.util.Date());
		if(user != null) {
			fileUpload.setUsersName(user.getLoginId());			
			fileUpload.setUserName(user.getLoginId());
		} 
		fileUpload = importExportService.saveFileUpload(fileUpload);
		if(fileUpload == null) {
			return new ResponseEntity<RestResult>(RestResult.getFailRestResult("File upload information save failed. "), headers, HttpStatus.INTERNAL_SERVER_ERROR); 
		} 
		System.out.println(model + " Saving the file metadata sucess. ");
		
		//storing file data in linux 
		RestResult restResult = importExportService.importFile(uploadfile, fileUpload.getFileId(), model);				
		if(RestResult.isSuccessRestResult(restResult)) {
			httpSession.setAttribute(QMSConstants.INPUT_FILE_ID, fileUpload.getFileId());
		} else {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.INTERNAL_SERVER_ERROR);	
		}
		//System.out.println(model + " Saved the file in Linux. ");
		
		//storing file data in hive 
		restResult = importExportService.callHivePatitioning(model);
		System.out.println(" Alter HIVE Partition success. ");
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.OK);
		} else {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@RequestMapping(value = "/export", method = RequestMethod.POST)
	public ResponseEntity<RestResult> exportFile(@RequestBody QMSFile file, UriComponentsBuilder ucBuilder) {
		
		RestResult restResult = importExportService.exportFile(file.getFile());
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
		}
		return new ResponseEntity<RestResult>(restResult, HttpStatus.INTERNAL_SERVER_ERROR);
	}	
	
	@RequestMapping(value = "/run_r/{modelType}", method = RequestMethod.GET)
	public ResponseEntity<RestResult> runRFile(@PathVariable("modelType") String modelType, UriComponentsBuilder ucBuilder) {
		
		List<String> modelList = Arrays.asList(new String[]{"noshow", "lhe", "lhc", "persona", "nc"});
		if(!modelList.contains(modelType)) {
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("Invalid model. Please select valid model. "),  
            		HttpStatus.BAD_REQUEST);			
		}		
		
		RestResult restResult = importExportService.runRFile(modelType);
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
	
	@RequestMapping(value = "/csv_output1", method = RequestMethod.GET)
	public ResponseEntity<Set<CSVOutPut1>> getOutputCSVData1() {
		System.out.println("Fetching Output.csv data ");
		Set<CSVOutPut1> setCSVOutPut = importExportService.getCSVOutPut1();
		return new ResponseEntity<Set<CSVOutPut1>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/csv_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getModelSummaryCSVData() {
		System.out.println("Fetching ModelSummary.csv data ");
		Set<ModelSummary> setCSVOutPut = importExportService.getCSVModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/csv_confusionMatric", method = RequestMethod.GET)
	public ResponseEntity<Set<ConfusionMatric>> getConfusionMatricCSVData() {
		System.out.println("Fetching ConfusionMatric.csv data ");
		Set<ConfusionMatric> setCSVOutPut = importExportService.getCSVConfusionMatric();
		return new ResponseEntity<Set<ConfusionMatric>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/csv_modelScore", method = RequestMethod.GET)
	public ResponseEntity<ModelScore> getModelScoreCSVData() {
		System.out.println("Fetching ModelScore.csv data ");
		ModelScore cSVOutPut = importExportService.getCSVModelScore();
		return new ResponseEntity<ModelScore>(cSVOutPut, HttpStatus.OK);
	}
	
	//////////////////////////NON Complience//////////////////////////////
	@RequestMapping(value = "/nc_output", method = RequestMethod.GET)
	public ResponseEntity<Set<LHEOutput>> getNCOutput() {
		Set<LHEOutput> setCSVOutPut = importExportService.getNCOutPut();
		return new ResponseEntity<Set<LHEOutput>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/nc_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getNCModelSummary() {
		Set<ModelSummary> setCSVOutPut = importExportService.getNCModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/nc_modelMatric", method = RequestMethod.GET)
	public ResponseEntity<ModelMetric> getNCConfusionMatric() {
		ModelMetric setCSVOutPut = importExportService.getNCModelMetric();
		return new ResponseEntity<ModelMetric>(setCSVOutPut, HttpStatus.OK);
	}
	
	//////////////////////////Complience//////////////////////////////	
	@RequestMapping(value = "/complience_output", method = RequestMethod.GET)
	public ResponseEntity<Set<ComplianceOutPut>> getComplienceOutput() {
		System.out.println("Fetching Output.csv data ");
		Set<ComplianceOutPut> setCSVOutPut = importExportService.getComplianceOutPut();
		return new ResponseEntity<Set<ComplianceOutPut>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/complience_modelSummary", method = RequestMethod.GET)
	public ResponseEntity<Set<ModelSummary>> getComplienceModelSummary() {
		System.out.println("Fetching ModelSummary.csv data ");
		Set<ModelSummary> setCSVOutPut = importExportService.getComplianceModelSummary();
		return new ResponseEntity<Set<ModelSummary>>(setCSVOutPut, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/complience_modelMatric", method = RequestMethod.GET)
	public ResponseEntity<ModelMetric> getComplienceConfusionMatric() {
		System.out.println("Fetching ConfusionMatric.csv data ");
		ModelMetric setCSVOutPut = importExportService.getComplianceModelMetric();
		return new ResponseEntity<ModelMetric>(setCSVOutPut, HttpStatus.OK);
	}	
}
