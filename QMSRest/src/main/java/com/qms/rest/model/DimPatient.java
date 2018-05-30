package com.qms.rest.model;

public class DimPatient {

	private String patId;
	private String ptyId;
	private String emrPatId;
	private String ssn;
	private String mrn;
	private String firstName;
	private String middleName;
	private String lastName;
	private String city;
	private String addLine1;
	private String addLine2;
	private String state;
	private String county;
	private String country;
	private String race;
	private String zip;
	private String deathDate;
	private String birthDate;
	private String emailAddress;
	private String maritialStatus;
	private String language;
	private String gender;
	private String lngtd;
	private String lattd;	
	private String ethniCity;
	private String currFlag;
	private String createDate;
	private String updateDate;
	
	//for other fields - AggregateFactMember
	private String comorbidity1;
	private String comorbidity2;
	private String comorbidity3;
	private String comorbidity4;
	private String comorbidity5;
	private String comorbidity6;
	private String comorbidity7;
	private String comorbidity8;
	private String comorbidity9;
	private String comorbidity10;
	private String careGaps1;
	private String careGaps2;
	private String careGaps3;
	private String careGaps4;
	private String ipVisitsCount;
	private String opVisitsCount;
	private String erVisitsCount;
	private String prescription;
	private String nextAppointmentDate;
	private String physicianName;
	private String department;
	private String procedureName1;
	private String procedureDateTime1;
	private String procedureName2;
	private String procedureDateTime2;	
	private String lastDateService;
	
	//for providerdetails
	private String providerFirstName;
	private String providerLastName;
	private String providerAddress1;
	private String providerAddress2;
	private String providerBillingTaxId;
	private String providerSpeciality;	
	
	//added fileds
	private String address;
	private String name;
	private String phone;
	private String age;
	private String primaryPayer;
	private String mraScore;
	private String risk;
	
	
	public String getRisk() {
		return risk;
	}
	public void setRisk(String risk) {
		this.risk = risk;
	}
	public String getMraScore() {
		return mraScore;
	}
	public void setMraScore(String mraScore) {
		this.mraScore = mraScore;
	}
	public String getPrimaryPayer() {
		return primaryPayer;
	}
	public void setPrimaryPayer(String primaryPayer) {
		this.primaryPayer = primaryPayer;
	}
	public String getPatId() {
		return patId;
	}
	public void setPatId(String patId) {
		this.patId = patId;
	}
	public String getPtyId() {
		return ptyId;
	}
	public void setPtyId(String ptyId) {
		this.ptyId = ptyId;
	}
	public String getEmrPatId() {
		return emrPatId;
	}
	public void setEmrPatId(String emrPatId) {
		this.emrPatId = emrPatId;
	}
	public String getSsn() {
		return ssn;
	}
	public void setSsn(String ssn) {
		this.ssn = ssn;
	}
	public String getMrn() {
		return mrn;
	}
	public void setMrn(String mrn) {
		this.mrn = mrn;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getMiddleName() {
		return middleName;
	}
	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getAddLine1() {
		return addLine1;
	}
	public void setAddLine1(String addLine1) {
		this.addLine1 = addLine1;
	}
	public String getAddLine2() {
		return addLine2;
	}
	public void setAddLine2(String addLine2) {
		this.addLine2 = addLine2;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCounty() {
		return county;
	}
	public void setCounty(String county) {
		this.county = county;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getRace() {
		return race;
	}
	public void setRace(String race) {
		this.race = race;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	public String getDeathDate() {
		return deathDate;
	}
	public void setDeathDate(String deathDate) {
		this.deathDate = deathDate;
	}
	public String getBirthDate() {
		return birthDate;
	}
	public void setBirthDate(String birthDate) {
		this.birthDate = birthDate;
	}
	public String getEmailAddress() {
		return emailAddress;
	}
	public void setEmailAddress(String emailAddress) {
		this.emailAddress = emailAddress;
	}
	public String getMaritialStatus() {
		return maritialStatus;
	}
	public void setMaritialStatus(String maritialStatus) {
		this.maritialStatus = maritialStatus;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getLngtd() {
		return lngtd;
	}
	public void setLngtd(String lngtd) {
		this.lngtd = lngtd;
	}
	public String getLattd() {
		return lattd;
	}
	public void setLattd(String lattd) {
		this.lattd = lattd;
	}
	public String getEthniCity() {
		return ethniCity;
	}
	public void setEthniCity(String ethniCity) {
		this.ethniCity = ethniCity;
	}
	public String getCurrFlag() {
		return currFlag;
	}
	public void setCurrFlag(String currFlag) {
		this.currFlag = currFlag;
	}
	public String getCreateDate() {
		return createDate;
	}
	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}
	public String getUpdateDate() {
		return updateDate;
	}
	public void setUpdateDate(String updateDate) {
		this.updateDate = updateDate;
	}
	public String getComorbidity1() {
		return comorbidity1;
	}
	public void setComorbidity1(String comorbidity1) {
		this.comorbidity1 = comorbidity1;
	}
	public String getComorbidity2() {
		return comorbidity2;
	}
	public void setComorbidity2(String comorbidity2) {
		this.comorbidity2 = comorbidity2;
	}
	public String getComorbidity3() {
		return comorbidity3;
	}
	public void setComorbidity3(String comorbidity3) {
		this.comorbidity3 = comorbidity3;
	}
	public String getComorbidity4() {
		return comorbidity4;
	}
	public void setComorbidity4(String comorbidity4) {
		this.comorbidity4 = comorbidity4;
	}
	public String getComorbidity5() {
		return comorbidity5;
	}
	public void setComorbidity5(String comorbidity5) {
		this.comorbidity5 = comorbidity5;
	}
	public String getComorbidity6() {
		return comorbidity6;
	}
	public void setComorbidity6(String comorbidity6) {
		this.comorbidity6 = comorbidity6;
	}
	public String getComorbidity7() {
		return comorbidity7;
	}
	public void setComorbidity7(String comorbidity7) {
		this.comorbidity7 = comorbidity7;
	}
	public String getComorbidity8() {
		return comorbidity8;
	}
	public void setComorbidity8(String comorbidity8) {
		this.comorbidity8 = comorbidity8;
	}
	public String getComorbidity9() {
		return comorbidity9;
	}
	public void setComorbidity9(String comorbidity9) {
		this.comorbidity9 = comorbidity9;
	}
	public String getComorbidity10() {
		return comorbidity10;
	}
	public void setComorbidity10(String comorbidity10) {
		this.comorbidity10 = comorbidity10;
	}
	public String getCareGaps1() {
		return careGaps1;
	}
	public void setCareGaps1(String careGaps1) {
		this.careGaps1 = careGaps1;
	}
	public String getCareGaps2() {
		return careGaps2;
	}
	public void setCareGaps2(String careGaps2) {
		this.careGaps2 = careGaps2;
	}
	public String getCareGaps3() {
		return careGaps3;
	}
	public void setCareGaps3(String careGaps3) {
		this.careGaps3 = careGaps3;
	}
	public String getCareGaps4() {
		return careGaps4;
	}
	public void setCareGaps4(String careGaps4) {
		this.careGaps4 = careGaps4;
	}
	public String getIpVisitsCount() {
		return ipVisitsCount;
	}
	public void setIpVisitsCount(String ipVisitsCount) {
		this.ipVisitsCount = ipVisitsCount;
	}
	public String getOpVisitsCount() {
		return opVisitsCount;
	}
	public void setOpVisitsCount(String opVisitsCount) {
		this.opVisitsCount = opVisitsCount;
	}
	public String getErVisitsCount() {
		return erVisitsCount;
	}
	public void setErVisitsCount(String erVisitsCount) {
		this.erVisitsCount = erVisitsCount;
	}
	public String getPrescription() {
		return prescription;
	}
	public void setPrescription(String prescription) {
		this.prescription = prescription;
	}
	public String getNextAppointmentDate() {
		return nextAppointmentDate;
	}
	public void setNextAppointmentDate(String nextAppointmentDate) {
		this.nextAppointmentDate = nextAppointmentDate;
	}
	public String getPhysicianName() {
		return physicianName;
	}
	public void setPhysicianName(String physicianName) {
		this.physicianName = physicianName;
	}
	public String getDepartment() {
		return department;
	}
	public void setDepartment(String department) {
		this.department = department;
	}
	public String getProcedureName1() {
		return procedureName1;
	}
	public void setProcedureName1(String procedureName1) {
		this.procedureName1 = procedureName1;
	}
	public String getProcedureDateTime1() {
		return procedureDateTime1;
	}
	public void setProcedureDateTime1(String procedureDateTime1) {
		this.procedureDateTime1 = procedureDateTime1;
	}
	public String getProcedureName2() {
		return procedureName2;
	}
	public void setProcedureName2(String procedureName2) {
		this.procedureName2 = procedureName2;
	}
	public String getProcedureDateTime2() {
		return procedureDateTime2;
	}
	public void setProcedureDateTime2(String procedureDateTime2) {
		this.procedureDateTime2 = procedureDateTime2;
	}
	public String getLastDateService() {
		return lastDateService;
	}
	public void setLastDateService(String lastDateService) {
		this.lastDateService = lastDateService;
	}
	public String getProviderFirstName() {
		return providerFirstName;
	}
	public void setProviderFirstName(String providerFirstName) {
		this.providerFirstName = providerFirstName;
	}
	public String getProviderLastName() {
		return providerLastName;
	}
	public void setProviderLastName(String providerLastName) {
		this.providerLastName = providerLastName;
	}
	public String getProviderAddress1() {
		return providerAddress1;
	}
	public void setProviderAddress1(String providerAddress1) {
		this.providerAddress1 = providerAddress1;
	}
	public String getProviderAddress2() {
		return providerAddress2;
	}
	public void setProviderAddress2(String providerAddress2) {
		this.providerAddress2 = providerAddress2;
	}
	public String getProviderBillingTaxId() {
		return providerBillingTaxId;
	}
	public void setProviderBillingTaxId(String providerBillingTaxId) {
		this.providerBillingTaxId = providerBillingTaxId;
	}
	public String getProviderSpeciality() {
		return providerSpeciality;
	}
	public void setProviderSpeciality(String providerSpeciality) {
		this.providerSpeciality = providerSpeciality;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}	
	
}
