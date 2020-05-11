// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.SortedMap;

import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;

public interface ISampledDataHandler extends IHandler {
	
	public TimeSeries readBlockData(String cluster, int ln, String guid, long begints, int beginHfOffset, long endts, int endHfOffset, int sampleSize) 
			throws TimeSeriesException;

	SortedMap<Integer, Long> getSampledDataHistogram(String cluster, int ln, String guid, int bins, long begints, int beginHfOffset,
			long endts, int endHfOffset, int sampleSize, ISketchHandler sketchHandler) throws TimeSeriesException;
	
}
