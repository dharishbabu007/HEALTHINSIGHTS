package com.qms.rest.controller;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLConnection;
import java.nio.charset.Charset;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.Param;
import com.qms.rest.model.RestResult;
import com.qms.rest.service.CloseGapsService;

@RestController
@RequestMapping("/closeGaps")
@CrossOrigin
public class CloseGapController {

	@Autowired
	CloseGapsService closeGapsService;	
	
	@RequestMapping(value = "/{memberId}/{measureId}", method = RequestMethod.GET)
	public ResponseEntity<CloseGaps> getCloseGaps(@PathVariable("memberId") String memberId, 
			@PathVariable("measureId") String measureId) {
		CloseGaps closeGaps = closeGapsService.getCloseGaps(memberId, measureId);
		if (closeGaps == null) {
			return new ResponseEntity<CloseGaps>(HttpStatus.NO_CONTENT);			
		}
		return new ResponseEntity<CloseGaps>(closeGaps, HttpStatus.OK);
	}
	
	@RequestMapping(value = "/{memberId}/{measureId}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> insertCloseGaps(@PathVariable("memberId") String memberId, 
			@PathVariable("measureId") String measureId,
			@RequestBody CloseGaps closeGaps, UriComponentsBuilder ucBuilder) {
		RestResult restResult = closeGapsService.insertCloseGaps(closeGaps, memberId, measureId);
		if(RestResult.isSuccessRestResult(restResult))
			return new ResponseEntity<RestResult>(restResult, HttpStatus.OK);		
		else	
			return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);		
	}	
	
	@RequestMapping(value = "/file_upload/{type}", method = RequestMethod.POST)
	public ResponseEntity<RestResult> importFile(@PathVariable("type") String type, 
			@RequestParam("file") MultipartFile uploadfile) {
		System.out.println(" CloseGapController - Uploading Gic File Upload for type --> " + type);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Access-Control-Allow-Origin", "*");		
		headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
		
		if (uploadfile.isEmpty()) {            
            return new ResponseEntity<RestResult>(RestResult.getFailRestResult("File is empty. Please select a valid file!"), headers, 
            		HttpStatus.BAD_REQUEST);
        }
		
		//storing file data in linux 
		RestResult restResult = closeGapsService.importFile(uploadfile, type);				
		if(RestResult.isSuccessRestResult(restResult)) {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.OK);
		} else {
			return new ResponseEntity<RestResult>(restResult, headers, HttpStatus.INTERNAL_SERVER_ERROR);	
		}
	}

	@RequestMapping(value = "/fileDownload", method = RequestMethod.GET)
	public void downloadFile(HttpServletResponse response, @RequestParam(value="filePath", required=true) String filePath) throws IOException {	
		String fileName = filePath;
		System.out.println("fileName::"+fileName);
		File file = null;
		file = new File(fileName);
		if (!file.exists()) {
			String errorMessage = "Sorry. The file you are looking for does not exist";
			System.out.println(errorMessage);
			OutputStream outputStream = response.getOutputStream();
			outputStream.write(errorMessage.getBytes(Charset.forName("UTF-8")));
			outputStream.close();
			return;
		}
		String mimeType = URLConnection.guessContentTypeFromName(file.getName());
		if (mimeType == null) {
			System.out.println("mimetype is not detectable, will take default");
			mimeType = "application/octet-stream";
		}
		System.out.println("mimetype : " + mimeType);
		response.setContentType(mimeType);
		response.setHeader("Content-Disposition", String.format("attachment; filename=\"" + file.getName() + "\""));
		response.setContentLength((int) file.length());
		InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
		FileCopyUtils.copy(inputStream, response.getOutputStream());
	}

	
}
