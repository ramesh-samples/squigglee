// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.List;

public class Pattern {
	private String cluster;
	private String pguid;
	private String dataType;
	private String dimensionality;
	private List<String> values;
	public String getPguid() {
		return pguid;
	}
	public void setPguid(String pguid) {
		this.pguid = pguid;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getDimensionality() {
		return dimensionality;
	}
	public void setDimensionality(String dimensionality) {
		this.dimensionality = dimensionality;
	}
	public List<String> getValues() {
		return values;
	}
	public void setValues(List<String> values) {
		this.values = values;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	} 
}
