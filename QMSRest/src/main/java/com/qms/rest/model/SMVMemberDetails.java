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
public class SMVMemberDetails {
	private String memberId; 
	private String name; 
	private String address; 
	private String phone; 
	private String emailAddress; 
	private String age;
	private String gender; 
	private String ethnicity; 
	private String income; 
	private String occupation;
	private String pcpName;
	private String pcpNpi;
	private String pcpSpeciality;
	private String pcpAddress;
	private String nextAppointmentDate; 
	private String departmentName; 
	private String noShowLikelihood; 
	private String noShow;
}
