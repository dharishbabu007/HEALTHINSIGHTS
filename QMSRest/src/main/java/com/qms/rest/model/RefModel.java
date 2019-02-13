package com.qms.rest.model;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;


@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RefModel {
	
	//private String refModelId;
	//private String modelName;
	private List<String> refModelNameList = new ArrayList<>();
	
}
