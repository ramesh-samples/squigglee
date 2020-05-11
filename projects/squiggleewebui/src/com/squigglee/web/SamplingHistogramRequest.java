// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

public class SamplingHistogramRequest {
	private String cluster;
	private String ln;
	private String parameter;
	private String start;
	private String startHfOffset;
	private String end;
	private String endHfOffset;
	private String sampleSize;
	private String samplingMethod;
	private String bins;
	private String index;
	private String datatype;
	
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public String getParameter() {
		return parameter;
	}
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public String getSampleSize() {
		return sampleSize;
	}
	public void setSampleSize(String sampleSize) {
		this.sampleSize = sampleSize;
	}
	public String getSamplingMethod() {
		return samplingMethod;
	}
	public void setSamplingMethod(String samplingMethod) {
		this.samplingMethod = samplingMethod;
	}
	public String getBins() {
		return bins;
	}
	public void setBins(String bins) {
		this.bins = bins;
	}
	public String getIndex() {
		return index;
	}
	public void setIndex(String index) {
		this.index = index;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public String getStartHfOffset() {
		return startHfOffset;
	}
	public void setStartHfOffset(String startHfOffset) {
		this.startHfOffset = startHfOffset;
	}
	public String getEndHfOffset() {
		return endHfOffset;
	}
	public void setEndHfOffset(String endHfOffset) {
		this.endHfOffset = endHfOffset;
	}
}
