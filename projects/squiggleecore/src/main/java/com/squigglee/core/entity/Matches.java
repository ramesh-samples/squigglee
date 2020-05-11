package com.squigglee.core.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Matches {

	private Pattern pattern;
	private double radius;
	private int topk;
	private List<TimeSeriesConfig> requestDomain = new ArrayList<TimeSeriesConfig>();
	private SortedMap<Double,List<Match>> matchResults = new TreeMap<Double, List<Match>>();
	private boolean dataLocal = false;
	private String errorMessage = null;
	
	public Matches() {}
	
	public Matches(Pattern pattern, List<TimeSeriesConfig> requestDomain, double radius, int topk, boolean dataLocal) {
		this.pattern = pattern;
		this.requestDomain = requestDomain;
		this.radius = radius;
		this.topk = topk;
		this.dataLocal = dataLocal;
	}
	
	public Pattern getPattern() {
		return pattern;
	}
	public void setPattern(Pattern pattern) {
		this.pattern = pattern;
	}
	public double getRadius() {
		return radius;
	}
	public void setRadius(double radius) {
		this.radius = radius;
	}
	public int getTopk() {
		return topk;
	}
	public void setTopk(int topk) {
		this.topk = topk;
	}

	public SortedMap<Double,List<Match>> getMatchResults() {
		return matchResults;
	}
	public void setMatchResults(SortedMap<Double,List<Match>> matches) {
		this.matchResults = getTopkMatches(matches, this.topk);
	}
	public List<TimeSeriesConfig> getRequestDomain() {
		return requestDomain;
	}
	public void setRequestDomain(List<TimeSeriesConfig> requestDomain) {
		this.requestDomain = requestDomain;
	}
	
	public synchronized void addMatches(SortedMap<Double,List<Match>> matches) {
		for (double dist : matches.keySet()) {
			for (Match m : matches.get(dist)) {
				if (!matchResults.containsKey(dist))
					matchResults.put(dist, new ArrayList<Match>());
				
				boolean alreadyFound = false;
				for (Match foundMatch : matchResults.get(dist)) {
					if (foundMatch.getStart() == m.getStart() && foundMatch.getValues().firstKey().equals(m.getValues().firstKey())) {
						alreadyFound = true;
						break;
					}
				}
				if (!alreadyFound)
					matchResults.get(dist).add(m);
			}
		}
		matchResults = getTopkMatches(matchResults, topk);
	}
	
	private SortedMap<Double,List<Match>> getTopkMatches(SortedMap<Double,List<Match>> matches, int topk) {
		SortedMap<Double,List<Match>> topkMatches = new TreeMap<Double,List<Match>>();
		int counter = 0;
		for (double dist : matches.keySet()) {
			for (Match match : matches.get(dist)) {
				if (!topkMatches.containsKey(dist))
					topkMatches.put(dist, new ArrayList<Match>());
				topkMatches.get(dist).add(match);
				if (counter++ == topk)
					return topkMatches;
			}
		}
		return topkMatches;
	}
	public boolean isDataLocal() {
		return dataLocal;
	}
	public void setDataLocal(boolean dataLocal) {
		this.dataLocal = dataLocal;
	}
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

}
