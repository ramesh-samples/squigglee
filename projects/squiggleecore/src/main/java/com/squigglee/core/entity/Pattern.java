package com.squigglee.core.entity;

import java.util.ArrayList;
import java.util.List;

public class Pattern {

	private String cluster = null;
	private String pguid = null;
	private List<String> vals = null;
	private String errorMessage = null;
	
	public Pattern() {}
	
	public Pattern(String cluster, String pguid, List<String> vals) {
		setCluster(cluster);
		setPguid(pguid);
		setVals(vals);
	}
	
	public Pattern(String cluster, String pguid) {
		setCluster(cluster);
		setPguid(pguid);
		vals = new ArrayList<String>();
	}
	
	public Pattern(List<String> vals) {
		setVals(vals);
	}
	
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public String getPguid() {
		return pguid;
	}
	public void setPguid(String pguid) {
		this.pguid = pguid;
	}
	public List<String> getVals() {
		return vals;
	}
	public void setVals(List<String> vals) {
		this.vals = vals;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
