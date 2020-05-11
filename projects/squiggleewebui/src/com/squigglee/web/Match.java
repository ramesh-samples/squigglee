// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class Match {

	private String matchCluster;
	private String matchLogicalNumber;
	private String matchParameter;
	private String matchStart;
	private String actualDistance;
	private List<String> results = new ArrayList<String>();
	public String getMatchLogicalNumber() {
		return matchLogicalNumber;
	}
	public void setMatchLogicalNumber(String matchLogicalNumber) {
		this.matchLogicalNumber = matchLogicalNumber;
	}
	public String getMatchParameter() {
		return matchParameter;
	}
	public void setMatchParameter(String matchParameter) {
		this.matchParameter = matchParameter;
	}
	public String getMatchStart() {
		return matchStart;
	}
	public void setMatchStart(String matchStart) {
		this.matchStart = matchStart;
	}
	public List<String> getResults() {
		return results;
	}
	public void setResults(List<String> results) {
		this.results = results;
	}
	public String getActualDistance() {
		return actualDistance;
	}
	public void setActualDistance(String actualDistance) {
		this.actualDistance = actualDistance;
	}
	public String getMatchCluster() {
		return matchCluster;
	}
	public void setMatchCluster(String matchCluster) {
		this.matchCluster = matchCluster;
	}
}
