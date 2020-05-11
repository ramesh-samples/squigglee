// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.SortedMap;

import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesException;

public interface ISketchHandler extends IIndexHandler {
	
	public long pointQuery(String cluster, int ln, String guid, Object val) throws TimeSeriesException;	
	public long rangeQuery(String cluster, int ln, String guid, Object startVal, Object endVal) throws TimeSeriesException;	
	public Object inverseQuery(String cluster, int ln, String guid, double quantile) throws TimeSeriesException;
	SortedMap<Integer, Long> getSketchHistogram(String cluster, int ln, String guid, int bins)  throws TimeSeriesException;
	Stats statistics(String cluster, int ln, String guid)	throws TimeSeriesException;
	
}
