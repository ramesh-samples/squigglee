// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

public class ConfigData {

	private String cluster;
	private String ln;
	private String parameter;
	private String start;
	private String startts;
	private String end;
	private String endts;
	private String datatype;
	private String frequency;
	private String indexes;
	private String replication;
	private String strategy;
	private String isPatternIndexed;
	private String isSketched;
	private String patternIndexSize;
	
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
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public String getReplication() {
		return replication;
	}
	public void setReplication(String replication) {
		this.replication = replication;
	}
	public String getStrategy() {
		return strategy;
	}
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}
	public String getIndexes() {
		return indexes;
	}
	public void setIndexes(String indexes) {
		this.indexes = indexes;
	}
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public String getStartts() {
		return startts;
	}
	public void setStartts(String startts) {
		this.startts = startts;
	}
	public String getEndts() {
		return endts;
	}
	public void setEndts(String endts) {
		this.endts = endts;
	}
	public String getIsPatternIndexed() {
		return isPatternIndexed;
	}
	public void setIsPatternIndexed(String isPatternIndexed) {
		this.isPatternIndexed = isPatternIndexed;
	}
	public String getIsSketched() {
		return isSketched;
	}
	public void setIsSketched(String isSketched) {
		this.isSketched = isSketched;
	}
	public String getPatternIndexSize() {
		return patternIndexSize;
	}
	public void setPatternIndexSize(String patternIndexSize) {
		this.patternIndexSize = patternIndexSize;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
}
