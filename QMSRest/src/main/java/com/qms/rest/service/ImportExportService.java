package com.qms.rest.service;

import java.util.Set;

import org.springframework.web.multipart.MultipartFile;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.CSVOutPut1;
import com.qms.rest.model.ConfusionMatric;
import com.qms.rest.model.FileUpload;
import com.qms.rest.model.ModelScore;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;

public interface ImportExportService {
	
	RestResult importFile(MultipartFile file, int fileId);
	RestResult exportFile(String fileName);
	RestResult runRFile(String modelType);
	
	Set<CSVOutPut> getCSVOutPut();
	Set<CSVOutPut1> getCSVOutPut1();
	Set<ModelSummary> getCSVModelSummary();
	
	Set<ConfusionMatric> getCSVConfusionMatric();	
	ModelScore getCSVModelScore();
	
	FileUpload saveFileUpload(FileUpload fileUpload);	
	RestResult callHivePatitioning ();
}
