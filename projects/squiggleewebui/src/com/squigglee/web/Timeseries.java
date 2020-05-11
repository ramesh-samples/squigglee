// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class Timeseries {

	private String cluster;
	private String ln;
	private String id;
	private String freq;
	private String startts;
	private String endts;
	private List<Datapair> data = new ArrayList<Datapair>();
	
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getFreq() {
		return freq;
	}
	public void setFreq(String freq) {
		this.freq = freq;
	}
	public String getStartts() {
		return startts;
	}
	public void setStartts(String startts) {
		this.startts = startts;
	}
	public List<Datapair> getData() {
		return data;
	}
	public void setData(List<Datapair> data) {
		this.data = data;
	}
	public String getEndts() {
		return endts;
	}
	public void setEndts(String endts) {
		this.endts = endts;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
}
