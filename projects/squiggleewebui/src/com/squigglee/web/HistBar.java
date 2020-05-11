// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

public class HistBar {

	public HistBar() {}
	public HistBar (int binnum, long bincount) {
		this.binnum = "" + binnum;
		this.bincount = "" + bincount;
	}
	private String binnum;
	private String bincount;
	
	public String getBinnum() {
		return binnum;
	}
	public void setBinnum(String binnum) {
		this.binnum = binnum;
	}
	public String getBincount() {
		return bincount;
	}
	public void setBincount(String binCount) {
		this.bincount = binCount;
	}
}
