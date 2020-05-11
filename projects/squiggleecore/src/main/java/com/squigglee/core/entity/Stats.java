// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

import java.util.Map;

public class Stats {
	
	private double min;
	private double max;
	private double first;
	private double last;
	private long count;
	//private double average;
	//private double sum;
	//private double variance;
	//private double skewness;
	//private double kurtosis;
	//private Map<Double,Double> quantiles;
	private String name;
	private Map<Long,Object> heavyHitters;
	private long sketchDomainSize;
	
	public Stats() {}
	
	public Stats(double min, double max, double first, double last, long count, String name, Map<Long,Object> heavyHitters) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.last = last;
		this.count = count;
		this.name = name;
		this.heavyHitters = heavyHitters;
	}
	
	public double getMin() {
		return min;
	}
	public void setMin(double min) {
		this.min = min;
	}
	public double getMax() {
		return max;
	}
	public void setMax(double max) {
		this.max = max;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	public Map<Long,Object> getHeavyHitters() {
		return heavyHitters;
	}

	public void setHeavyHitters(Map<Long,Object> heavyHitters) {
		this.heavyHitters = heavyHitters;
	}

	public long getSketchDomainSize() {
		return sketchDomainSize;
	}

	public void setSketchDomainSize(long sketchDomainSize) {
		this.sketchDomainSize = sketchDomainSize;
	}

	public double getFirst() {
		return first;
	}

	public void setFirst(double first) {
		this.first = first;
	}

	public double getLast() {
		return last;
	}

	public void setLast(double last) {
		this.last = last;
	}
}
