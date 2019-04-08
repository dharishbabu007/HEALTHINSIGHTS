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
public class MemberCareGapsList {
	private String member_id;
	private String name;
	private String age;
	private String gender;
	private Integer countOfCareGaps;
	private String riskGrade;
    private List<MemberCareGaps> members;
    private String compliancePotential;
    private String measureSK;
    private List<String> careGaps=new ArrayList();
}
