package com.qms.rest.controller;

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
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.qms.rest.model.ClusterData;
import com.qms.rest.model.Program;
import com.qms.rest.model.ProgramEdit;
import com.qms.rest.model.QualityProgramUI;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.ProgramService;
import com.qms.rest.util.CustomErrorType;

@RestController
@RequestMapping("/program")
@CrossOrigin
public class ProgramController {
	
	public static final Logger logger = LoggerFactory.getLogger(QMSController.class);
	
	@Autowired
	private ProgramService programService;
	
	@RequestMapping(value = "/createProgram", method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<RestResult> createProgram(@RequestBody Program program) {
		String response = "Program created successfully for ProgramName : "+program.getProgramName();
		logger.info("About to create program :  " + program);
		programService.createProgram(program);
		
		RestResult restResult = RestResult.getSucessRestResult(response);		
		return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/program/{programId}", method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<?> getProgramById(@PathVariable("programId") int programId) {

		QualityProgramUI qualityProgram = programService.getProgramById(programId);
		if (qualityProgram == null) {
			logger.error(" Unable to fetch program details");
			return new ResponseEntity(new CustomErrorType("Program not found."), HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<QualityProgramUI>(qualityProgram, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/getProgramByName/{programName}", method = RequestMethod.GET)
	public ResponseEntity<ProgramEdit> getProgramByName(@PathVariable("programName") String programName) {
		ProgramEdit programEdit = programService.getProgramByName(programName);
		if(programEdit == null) {
			return new ResponseEntity<ProgramEdit>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<ProgramEdit>(programEdit, HttpStatus.OK);
	}	
	
	@RequestMapping(value = "/editProgram", method = RequestMethod.POST)
	public ResponseEntity<RestResult> editProgram(@RequestBody ProgramEdit program) {
		RestResult restResult = programService.editProgram(program);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
	}	
	
}
