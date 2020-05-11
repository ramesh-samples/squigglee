// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers;

import java.io.IOException;
import java.util.List;

import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;

public interface ITimeSeriesHandler {
	
	public byte[] fetchBulkData(int ln, String guid, long startts, long endts)  throws TimeSeriesException;
	public byte[] fetchMatches(List<TimeSeriesConfig> mdList, double[] pattern_norm, int topk, double radius) 
			throws TimeSeriesException;
	public int[] updateBulkData(byte[] bulkData) throws TimeSeriesException, IOException;
	public int[] updateMasterData(TimeSeriesConfig config) throws TimeSeriesException;
	List<TimeSeriesConfig> fetchMasterData(int ln, String guid) throws TimeSeriesException;
}
