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
public class NCOutput {
	private String patientId;
	private String patientName;
	private String age;                     
	private String gender;                  
	private String race;                    
	private String ethnicity;               
	private String maritalStatus;
	private String distanceNearestHC;      
	private String state;                   
	private String zipCode;                
	private String country;                 
	private String pcpAssignment;          
	private String noNCMeasures;   
	private String ncHistroy;       
	private String logOdds;                 
	private String predictedNC;
}
