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
public class ClusterData {

//	Set<ClusterCateVar> clusterCateVars;	
//	Set<ClusterContVar> clusterContVars;
	
	PersonaDefine clusterPersona;	
	PersonaClusterFeatures personaClusterFeatures;
}
