// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class SketchPageJson {

	private String cluster;
	private String ln;
	private String spParameter;
	private String spMin;
	private String spMax;
	private String spFirst;
	private String spLast;
	private String spCount;
	private String spPointQuery;
	private String spPointResult;
	private String spRangeQuery1;
	private String spRangeQuery2;
	private String spRangeResult;
	private String spInverseQuery;
	private String spInverseResult;
	private List<HistBar> spHistogram = new ArrayList<HistBar>();
	private String bins;
	private String spTopk;
	private List<String> spTopkValues = new ArrayList<String>();
	private String spSketchName;
	private String dataType;
	private SamplingHistogramRequest samplingHistogramRequest = new SamplingHistogramRequest();
	private List<HistBar> sampledDataHistogram = new ArrayList<HistBar>();
	private List<ConfigData> nodeConfig = new ArrayList<ConfigData>();
	private WebAction action;
	private String message;
	private String error;
	
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public String getSpParameter() {
		return spParameter;
	}
	public void setSpParameter(String spParameter) {
		this.spParameter = spParameter;
	}
	public String getSpMin() {
		return spMin;
	}
	public void setSpMin(String spMin) {
		this.spMin = spMin;
	}
	public String getSpMax() {
		return spMax;
	}
	public void setSpMax(String spMax) {
		this.spMax = spMax;
	}
	public String getSpCount() {
		return spCount;
	}
	public void setSpCount(String spCount) {
		this.spCount = spCount;
	}
	public String getSpPointQuery() {
		return spPointQuery;
	}
	public void setSpPointQuery(String spPointQuery) {
		this.spPointQuery = spPointQuery;
	}
	public String getSpPointResult() {
		return spPointResult;
	}
	public void setSpPointResult(String spPointResult) {
		this.spPointResult = spPointResult;
	}
	public String getSpRangeQuery1() {
		return spRangeQuery1;
	}
	public void setSpRangeQuery1(String spRangeQuery1) {
		this.spRangeQuery1 = spRangeQuery1;
	}
	public String getSpRangeQuery2() {
		return spRangeQuery2;
	}
	public void setSpRangeQuery2(String spRangeQuery2) {
		this.spRangeQuery2 = spRangeQuery2;
	}
	public String getSpRangeResult() {
		return spRangeResult;
	}
	public void setSpRangeResult(String spRangeResult) {
		this.spRangeResult = spRangeResult;
	}
	public List<HistBar> getSpHistogram() {
		return spHistogram;
	}
	public void setSpHistogram(List<HistBar> spHistogram) {
		this.spHistogram = spHistogram;
	}
	public String getSpTopk() {
		return spTopk;
	}
	public void setSpTopk(String spTopk) {
		this.spTopk = spTopk;
	}
	public List<String> getSpTopkValues() {
		return spTopkValues;
	}
	public void setSpTopkValues(List<String> spTopkValues) {
		this.spTopkValues = spTopkValues;
	}
	public String getSpSketchName() {
		return spSketchName;
	}
	public void setSpSketchName(String spSketchName) {
		this.spSketchName = spSketchName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
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
	public String getBins() {
		return bins;
	}
	public void setBins(String bins) {
		this.bins = bins;
	}
	public SamplingHistogramRequest getSamplingHistogramRequest() {
		return samplingHistogramRequest;
	}
	public void setSamplingHistogramRequest(SamplingHistogramRequest samplingHistogramRequest) {
		this.samplingHistogramRequest = samplingHistogramRequest;
	}
	public List<HistBar> getSampledDataHistogram() {
		return sampledDataHistogram;
	}
	public void setSampledDataHistogram(List<HistBar> sampledDataHistogram) {
		this.sampledDataHistogram = sampledDataHistogram;
	}
	public List<ConfigData> getNodeConfig() {
		return nodeConfig;
	}
	public void setNodeConfig(List<ConfigData> nodeConfig) {
		this.nodeConfig = nodeConfig;
	}
	public String getSpInverseQuery() {
		return spInverseQuery;
	}
	public void setSpInverseQuery(String spInverseQuery) {
		this.spInverseQuery = spInverseQuery;
	}
	public String getSpInverseResult() {
		return spInverseResult;
	}
	public void setSpInverseResult(String spInverseResult) {
		this.spInverseResult = spInverseResult;
	}
	public String getSpFirst() {
		return spFirst;
	}
	public void setSpFirst(String spFirst) {
		this.spFirst = spFirst;
	}
	public String getSpLast() {
		return spLast;
	}
	public void setSpLast(String spLast) {
		this.spLast = spLast;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	
}
