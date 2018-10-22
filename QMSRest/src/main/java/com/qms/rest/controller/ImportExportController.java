package com.qms.rest.controller;

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
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.FileUpload;
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
	
	@RequestMapping(value = "/import", method = RequestMethod.POST)
	public ResponseEntity<RestResult> importFile(@RequestParam("file") MultipartFile uploadfile) {
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Access-Control-Allow-Origin", "*");		
		headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");		
		
		if (uploadfile.isEmpty()) {            
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("File is empty. Please select a valid file!"), headers, 
            		HttpStatus.BAD_REQUEST);
        }
		
		String extension = FilenameUtils.getExtension(uploadfile.getOriginalFilename());
		if(!extension.equalsIgnoreCase("CSV")) {
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("Invalid file. File type should be CSV. "), headers, 
            		HttpStatus.BAD_REQUEST);			
		}
		
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
		
		RestResult restResult = importExportService.importFile(uploadfile, fileUpload.getFileId());				
		if(RestResult.isSuccessRestResult(restResult)) {
			httpSession.setAttribute(QMSConstants.INPUT_FILE_ID, fileUpload.getFileId());
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.OK);
		}
		restResult = importExportService.callHivePatitioning();
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
	
	@RequestMapping(value = "/run_r/{modelType}", method = RequestMethod.GET)
	public ResponseEntity<RestResult> runRFile(@PathVariable("modelType") String modelType, UriComponentsBuilder ucBuilder) {
		
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
	
	
}
