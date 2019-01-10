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
public class PersonaClusterFeatures {
	String clusterId;
	String featureName;
	String featureType;
	String featureSignificanceValue;
	String maxFrequency;
	String modelId;
	String x;
	int y;
	String personaName;
	
	@Override
	public int hashCode() {
		return clusterId.hashCode()+featureName.hashCode()+featureType.hashCode()+
				featureSignificanceValue.hashCode()+
				maxFrequency.hashCode()+modelId.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PersonaClusterFeatures) {
			PersonaClusterFeatures pp = (PersonaClusterFeatures) obj;
            return (pp.clusterId.equals(this.clusterId) && pp.featureName.equals(this.featureName) && 
            		pp.featureType.equals(this.featureType) && pp.featureSignificanceValue.equals(this.featureSignificanceValue) && 
            		pp.maxFrequency.equals(this.maxFrequency) && pp.modelId.equals(this.modelId));
        } else {
            return false;
        }
	}
}
