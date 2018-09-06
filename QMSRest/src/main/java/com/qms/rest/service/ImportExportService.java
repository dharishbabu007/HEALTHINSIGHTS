package com.qms.rest.service;

import java.util.Set;

import com.qms.rest.model.CSVOutPut;
import com.qms.rest.model.ModelSummary;
import com.qms.rest.model.RestResult;

public interface ImportExportService {
	
	RestResult importFile(String fileName);
	RestResult exportFile(String fileName);
	RestResult runRFile(String fileName);
	
	Set<CSVOutPut> getCSVOutPut();
	Set<ModelSummary> getCSVModelSummary();
	
}
