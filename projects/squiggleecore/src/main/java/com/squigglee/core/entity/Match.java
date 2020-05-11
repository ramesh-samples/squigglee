// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import java.util.SortedMap;

public class Match {
	private String id = null, cluster = null, dataType = null, frequency = null;
	private long start;
	private double distance;
	private int ln;
	private SortedMap<Long, Object> values;
	
	public Match() {} 
	
	public Match(String cluster, int ln, String id, String dataType, String frequency, long start, SortedMap<Long, Object> values, double distance) {
		this.setCluster(cluster);
		this.setLn(ln);
		this.setId(id);
		this.setDataType(dataType);
		this.setFrequency(frequency);
		this.setStart(start);
		this.setValues(values);
		this.distance = distance;
	}
	
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	public SortedMap<Long, Object> getValues() {
		return values;
	}
	public void setValues(SortedMap<Long, Object> values) {
		this.values = values;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getLn() {
		return ln;
	}

	public void setLn(int ln) {
		this.ln = ln;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
}
