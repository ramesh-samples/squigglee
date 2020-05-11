// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.util.ArrayList;
import java.util.List;

public class MatchPageJson {
	private String cluster;
	private String ln;
	private String tsPanelDimensionality;
	private String tsPanelParameter;
	private String tsPanelPatternName;
	private String tsPanelStart;
	private String tsPanelEnd;
	private String tsPanelStartts;
	private String tsPanelEndts;
	private String tsPanelStartHfOffset;
	private String tsPanelEndHfOffset;
	private String last;
	private String tsWindowSize;
	private String tsViewStartts;
	private String tsViewEndts;
	private String tsViewStartHfOffset;
	private String tsViewEndHfOffset;
	private String ptrnPanelLn;
	private String ptrnPanelParameter;
	private String ptrnPanelStart;
	private String ptrnPanelEnd;
	private String ptrnPanelDimensionality;
	private String ptrnPanelPatternName;
	private String dataType;
	private String frequency;
	private String matchCount = "6";
	private String matchDistance;
	private WebAction action;
	private String error;
	private String message;
	private Timeseries tsResults;
	private List<String> pattern = new ArrayList<String>();
	private List<Match> matches = new ArrayList<Match>();
	private List<ConfigData> nodeConfig = new ArrayList<ConfigData>();
	private CapturedPatterns capturedPatterns = new CapturedPatterns();
	private List<ConfigData> searchSelections = new ArrayList<ConfigData>();
	
	public String getTsPanelDimensionality() {
		return tsPanelDimensionality;
	}
	public void setTsPanelDimensionality(String tsPanelDimensionality) {
		this.tsPanelDimensionality = tsPanelDimensionality;
	}
	public String getLn() {
		return ln;
	}
	public void setLn(String ln) {
		this.ln = ln;
	}
	public String getTsPanelParameter() {
		return tsPanelParameter;
	}
	public void setTsPanelParameter(String tsPanelParameter) {
		this.tsPanelParameter = tsPanelParameter;
	}
	public String getTsPanelPatternName() {
		return tsPanelPatternName;
	}
	public void setTsPanelPatternName(String tsPanelPatternName) {
		this.tsPanelPatternName = tsPanelPatternName;
	}
	public String getTsPanelStart() {
		return tsPanelStart;
	}
	public void setTsPanelStart(String tsPanelStart) {
		this.tsPanelStart = tsPanelStart;
	}
	public String getTsPanelEnd() {
		return tsPanelEnd;
	}
	public void setTsPanelEnd(String tsPanelEnd) {
		this.tsPanelEnd = tsPanelEnd;
	}
	public String getPtrnPanelLn() {
		return ptrnPanelLn;
	}
	public void setPtrnPanelLn(String ptrnPanelLn) {
		this.ptrnPanelLn = ptrnPanelLn;
	}
	public String getPtrnPanelParameter() {
		return ptrnPanelParameter;
	}
	public void setPtrnPanelParameter(String ptrnPanelParameter) {
		this.ptrnPanelParameter = ptrnPanelParameter;
	}
	public String getPtrnPanelStart() {
		return ptrnPanelStart;
	}
	public void setPtrnPanelStart(String ptrnPanelStart) {
		this.ptrnPanelStart = ptrnPanelStart;
	}
	public String getPtrnPanelEnd() {
		return ptrnPanelEnd;
	}
	public void setPtrnPanelEnd(String ptrnPanelEnd) {
		this.ptrnPanelEnd = ptrnPanelEnd;
	}
	public String getPtrnPanelDimensionality() {
		return ptrnPanelDimensionality;
	}
	public void setPtrnPanelDimensionality(String ptrnPanelDimensionality) {
		this.ptrnPanelDimensionality = ptrnPanelDimensionality;
	}
	public String getPtrnPanelPatternName() {
		return ptrnPanelPatternName;
	}
	public void setPtrnPanelPatternName(String ptrnPanelPatternName) {
		this.ptrnPanelPatternName = ptrnPanelPatternName;
	}
	public Timeseries getTsResults() {
		return tsResults;
	}
	public void setTsResults(Timeseries tsResults) {
		this.tsResults = tsResults;
	}
	public List<String> getPattern() {
		return pattern;
	}
	public void setPattern(List<String> pattern) {
		this.pattern = pattern;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public List<Match> getMatches() {
		return matches;
	}
	public void setMatches(List<Match> matches) {
		this.matches = matches;
	}
	public WebAction getAction() {
		return action;
	}
	public void setAction(WebAction action) {
		this.action = action;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getMatchDistance() {
		return matchDistance;
	}
	public void setMatchDistance(String matchDistance) {
		this.matchDistance = matchDistance;
	}
	public String getMatchCount() {
		return matchCount;
	}
	public void setMatchCount(String matchCount) {
		this.matchCount = matchCount;
	}
	public CapturedPatterns getCapturedPatterns() {
		return capturedPatterns;
	}
	public void setCapturedPatterns(CapturedPatterns capturedPatterns) {
		this.capturedPatterns = capturedPatterns;
	}
	public List<ConfigData> getNodeConfig() {
		return nodeConfig;
	}
	public void setNodeConfig(List<ConfigData> nodeConfig) {
		this.nodeConfig = nodeConfig;
	}
	public List<ConfigData> getSearchSelections() {
		return searchSelections;
	}
	public void setSearchSelections(List<ConfigData> searchSelections) {
		this.searchSelections = searchSelections;
	}
	public String getTsWindowSize() {
		return tsWindowSize;
	}
	public void setTsWindowSize(String tsWindowSize) {
		this.tsWindowSize = tsWindowSize;
	}
	public String getTsPanelStartts() {
		return tsPanelStartts;
	}
	public void setTsPanelStartts(String tsPanelStartts) {
		this.tsPanelStartts = tsPanelStartts;
	}
	public String getTsPanelEndts() {
		return tsPanelEndts;
	}
	public void setTsPanelEndts(String tsPanelEndts) {
		this.tsPanelEndts = tsPanelEndts;
	}
	public String getTsViewStartts() {
		return tsViewStartts;
	}
	public void setTsViewStartts(String tsViewStartts) {
		this.tsViewStartts = tsViewStartts;
	}
	public String getTsViewEndts() {
		return tsViewEndts;
	}
	public void setTsViewEndts(String tsViewEndts) {
		this.tsViewEndts = tsViewEndts;
	}
	public String getLast() {
		return last;
	}
	public void setLast(String last) {
		this.last = last;
	}
	public String getTsViewStartHfOffset() {
		return tsViewStartHfOffset;
	}
	public void setTsViewStartHfOffset(String tsViewStartHfOffset) {
		this.tsViewStartHfOffset = tsViewStartHfOffset;
	}
	public String getTsViewEndHfOffset() {
		return tsViewEndHfOffset;
	}
	public void setTsViewEndHfOffset(String tsViewEndHfOffset) {
		this.tsViewEndHfOffset = tsViewEndHfOffset;
	}
	public String getTsPanelStartHfOffset() {
		return tsPanelStartHfOffset;
	}
	public void setTsPanelStartHfOffset(String tsPanelStartHfOffset) {
		this.tsPanelStartHfOffset = tsPanelStartHfOffset;
	}
	public String getTsPanelEndHfOffset() {
		return tsPanelEndHfOffset;
	}
	public void setTsPanelEndHfOffset(String tsPanelEndHfOffset) {
		this.tsPanelEndHfOffset = tsPanelEndHfOffset;
	}
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	
}
