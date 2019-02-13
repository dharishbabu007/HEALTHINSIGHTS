package com.qms.rest.model;

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
public class CloseGaps {
	private String memberId;
	private String gender;
	private String name;
	private String dateOfBirth;
	
	private String careGap;
	private String openDate;
	private String targetDate;
	private String assignedTo;
	private String status;
	private String lastActionDate;
	private String nextAppointmentDate;
	
	private Set<CloseGap> careGaps;
}
