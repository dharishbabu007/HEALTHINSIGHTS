package com.qms.rest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class FileDownload {

	private String filePath;
	private String file;
	private String type;
	private String typeId;
	
}
