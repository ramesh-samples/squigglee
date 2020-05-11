package com.squigglee.core.entity;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class Operators {

	private Map<String, String> vars = null;
	private List<String> funcs = null;
	private long start, end;
	private int startHfOffset, endHfOffset;
	private List<SortedMap<Long, Object>> computedResults = null;
	private String errorMessage = null;
	
	public Operators() {}
	
	public Operators(Map<String, String> vars, List<String> funcs, long start, int startHfOffset, long end, int endHfOffset) {
		this.setVars(vars);
		this.setFuncs(funcs);
		this.setStart(start);
		this.setStartHfOffset(startHfOffset);
		this.setEnd(end);
		this.setEndHfOffset(endHfOffset);
	}

	public Map<String, String> getVars() {
		return vars;
	}

	public void setVars(Map<String, String> vars) {
		this.vars = vars;
	}

	public List<String> getFuncs() {
		return funcs;
	}

	public void setFuncs(List<String> funcs) {
		this.funcs = funcs;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public int getStartHfOffset() {
		return startHfOffset;
	}

	public void setStartHfOffset(int startHfOffset) {
		this.startHfOffset = startHfOffset;
	}

	public int getEndHfOffset() {
		return endHfOffset;
	}

	public void setEndHfOffset(int endHfOffset) {
		this.endHfOffset = endHfOffset;
	}

	public List<SortedMap<Long, Object>> getComputedResults() {
		return computedResults;
	}

	public void setComputedResults(List<SortedMap<Long, Object>> computedResults) {
		this.computedResults = computedResults;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
}
