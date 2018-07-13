package com.qms.rest.service;

import java.util.Set;

import com.qms.rest.model.Measure;
import com.qms.rest.model.MeasureCreator;
import com.qms.rest.model.NameValue;
import com.qms.rest.model.RestResult;

public interface QMSService {

	Set<MeasureCreator> getMeasureLibrary(String programName, String value);
	
	MeasureCreator getMeasureLibraryById(int id);
	
	Set<String> getMeasureDropDownList(String tableName, String columnName);
	
	Set<NameValue> getMeasureDropDownNameValueList(String tableName, String columnValue, String columnName);
	
	Set<String> getQMSHomeDropDownList(String tableName, String columnName);
	
	RestResult insertMeasureCreator(MeasureCreator measureCreator);
	
	RestResult updateMeasureCreator(MeasureCreator measureCreator);
	
	MeasureCreator findMeasureCreatorById(int id);
	
	Set<MeasureCreator> getAllWorkList();
	
	RestResult updateMeasureWorkListStatus(int id, String status);
}
