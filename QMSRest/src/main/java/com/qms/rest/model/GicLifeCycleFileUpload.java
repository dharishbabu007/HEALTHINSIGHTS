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
public class GicLifeCycleFileUpload {
//	private int fileId;
//	private int lifeCycleId;
	private String filePath;
	private String fileName;
	private String type;
	private String typeId;
}