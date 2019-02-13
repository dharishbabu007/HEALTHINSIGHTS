package com.qms.rest.model;

import java.util.List;
import java.util.Set;

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

public class CareGapMeasure {
	//private String maasureName; 
	
	private Set<String> cearGapMeasureList;
}
