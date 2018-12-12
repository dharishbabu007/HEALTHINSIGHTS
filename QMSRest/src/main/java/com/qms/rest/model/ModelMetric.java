package com.qms.rest.model;

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
public class ModelMetric {

	private String tp;
	private String fp;
	private String tn;
	private String fn;
	private String score;
	private String imagePath;
}
