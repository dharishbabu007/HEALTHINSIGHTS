package com.qms.rest.model;


import lombok.*;

import java.util.Date;
import java.util.List;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Program {

    private String programName;

    private Date startDate;

    private Date endDate;

    private List<ProgramCategory> programCategorys;
}
