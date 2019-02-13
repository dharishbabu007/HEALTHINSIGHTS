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
public class CloseGap {
	private int lifeCycleId;
	private String measureTitle;
	private String qualityMeasureId;
	private String intervention;
	private String priority;
	private String payerComments;
	private String providerComments;
	private String status;
	private String gapDate;
	private String targetDate;
	private String closeGap;
	private String actionCareGap;
	private List<String> uploadList = new ArrayList<>();
}
