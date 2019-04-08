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
public class User {
	private String id;
	private String name;
	private String email;
	private String roleId;
	private String loginId;
	private String firstName;
	private String lastName;
	private String securityQuestion;
	private String securityAnswer;
	private String phoneNumber;
	private String password;	
	private String resetPassword;
	private String status;
}
