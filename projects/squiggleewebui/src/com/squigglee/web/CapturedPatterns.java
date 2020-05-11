// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class CapturedPatterns {
	private String cluster = null;
	private List<String> doublePatterns = new ArrayList<String>();
	private List<String> intPatterns = new ArrayList<String>();
	private List<String> floatPatterns = new ArrayList<String>();
	private List<String> longPatterns = new ArrayList<String>();
	private List<String> booleanPatterns = new ArrayList<String>();
	
	public List<String> getDoublePatterns() {
		return doublePatterns;
	}
	public void setDoublePatterns(List<String> doublePatterns) {
		this.doublePatterns = doublePatterns;
	}
	public List<String> getIntPatterns() {
		return intPatterns;
	}
	public void setIntPatterns(List<String> intPatterns) {
		this.intPatterns = intPatterns;
	}
	public List<String> getFloatPatterns() {
		return floatPatterns;
	}
	public void setFloatPatterns(List<String> floatPatterns) {
		this.floatPatterns = floatPatterns;
	}
	public List<String> getLongPatterns() {
		return longPatterns;
	}
	public void setLongPatterns(List<String> longPatterns) {
		this.longPatterns = longPatterns;
	}
	public List<String> getBooleanPatterns() {
		return booleanPatterns;
	}
	public void setBooleanPatterns(List<String> booleanPatterns) {
		this.booleanPatterns = booleanPatterns;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
}
