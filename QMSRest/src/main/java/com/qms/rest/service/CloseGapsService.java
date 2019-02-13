package com.qms.rest.service;

import org.springframework.web.multipart.MultipartFile;

import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.GicLifeCycleFileUpload;
import com.qms.rest.model.RestResult;

public interface CloseGapsService {

	CloseGaps getCloseGaps(String memberId, String measureId);
	RestResult insertCloseGaps(CloseGaps closeGaps, String memberId, String measureId);
	RestResult importFile(MultipartFile file);
	RestResult saveFileUpload(GicLifeCycleFileUpload fileUpload);
	
}
