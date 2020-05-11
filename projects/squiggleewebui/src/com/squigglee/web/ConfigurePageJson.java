// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class ConfigurePageJson {
	private String cluster;
	private String ln;
	private List<ConfigData> nodeConfig = new ArrayList<ConfigData>();
	private WebAction action;
	
	private String sampleStartOfToday;
	private String sampleEndOfToday;
	
	private String message;
	private String error;
	
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public WebAction getAction() {
		return action;
	}
	public void setAction(WebAction action) {
		this.action = action;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public List<ConfigData> getNodeConfig() {
		return nodeConfig;
	}
	public void setNodeConfig(List<ConfigData> nodeConfig) {
		this.nodeConfig = nodeConfig;
	}
	public String getSampleEndOfToday() {
		return sampleEndOfToday;
	}
	public void setSampleEndOfToday(String sampleEndOfToday) {
		this.sampleEndOfToday = sampleEndOfToday;
	}
	public String getSampleStartOfToday() {
		return sampleStartOfToday;
	}
	public void setSampleStartOfToday(String sampleStartOfToday) {
		this.sampleStartOfToday = sampleStartOfToday;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
}
