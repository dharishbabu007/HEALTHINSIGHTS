package com.qms.rest.model;

import java.util.Date;
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
public class ProgramEdit {
	private int programId;
    private String programName;
    private String startDate;
    private String endDate;
    private List<ProgramCategoryEdit> programCategorys;
}
