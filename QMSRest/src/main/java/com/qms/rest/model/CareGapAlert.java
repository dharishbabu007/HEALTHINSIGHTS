package com.qms.rest.model;

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
public class CareGapAlert implements Comparable<CareGapAlert> {
	private String careGap;
	private List<Integer> alerts;
	
	@Override
	public int compareTo(CareGapAlert arg0) {		
		return careGap.compareTo(arg0.getCareGap());
	}

}
