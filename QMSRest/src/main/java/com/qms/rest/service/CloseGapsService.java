package com.qms.rest.service;

import com.qms.rest.model.CloseGaps;
import com.qms.rest.model.RestResult;

public interface CloseGapsService {

	CloseGaps getCloseGaps(String memberId);
	RestResult insertCloseGaps(CloseGaps closeGaps, String memberId);
	
}
