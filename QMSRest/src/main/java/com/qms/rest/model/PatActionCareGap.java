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
public class PatActionCareGap {
	Set<String> valueSet;
	Set<String> codeType;
	Set<String> codes;
}
