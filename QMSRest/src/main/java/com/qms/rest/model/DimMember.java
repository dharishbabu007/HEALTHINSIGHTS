package com.qms.rest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name ="DIM_MEMBER")
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimMember {

    @Column(name = "MEMBER_ID")
    @Id
    private int memberId;

    @Column(name = "MEMBER_SK")
    private String memberSk;

    @Column(name = "GENDER")
    private String gender;

    @Column(name = "DATE_OF_BIRTH_SK")
    private String dateOfBirthSk;

    @Column(name = "DATE_OF_DEATH_SK")
    private String dateOfDeathSk;

    @Column(name = "FIRST_NAME")
    private String firstName;

    @Column(name = "MIDDLE_NAME")
    private String middelName;

    @Column(name = "LAST_NAME")
    private String lastName;

    @Column(name = "ADDRESS1")
    private String address1;

    @Column(name = "ADDRESS2")
    private String address2;

    @Column(name = "CITY")
    private String city;

    @Column(name = "STATE")
    private String state;

    @Column(name = "ZIP")
    private String zip;

    @Column(name = "COUNTY_NAME")
    private String countryName;

    @Column(name = "PHONE")
    private String phone;

    @Column(name = "EMAIL_ADDRESS")
    private String emailAddress;

    @Column(name = "MARITAL_STATUS")
    private String maritalStatus;

    @Column(name = "LANGUAGE")
    private String language;

    @Column(name = "LNGTD")
    private String lngId;

    @Column(name = "LATTD")
    private String latId;

    @Column(name = "ETHNICITY")
    private String ethnicity;

    @Column(name = "IMAGE_PATH")
    private String imagePath;

    @Column(name = "CURR_FLAG")
    private String currentFlag;

    @Column(name = "REC_CREATE_DATE")
    private Date recCreateDate;

    @Column(name = "LATEST_FLAG")
    private String latestFlag;


    @Column(name = "ACTIVE_FLAG")
    private String activeFlag;


    @Column(name = "INGESTION_DATE")
    private Date ingestionDate;


    @Column(name = "SOURCE")
    private String source;


    @Column(name = "USER")
    private String user;

}
