// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.TimeSeriesException;

public interface IPatternHandler extends IIndexHandler {
	
	public List<Long> fetchCandidatePatterns(MasterData md, String idxTableName, List<Integer> hashes) throws TimeSeriesException;
	public Map<Long, SortedMap<Long, Object>> fetchCandidateTimeSeries(MasterData md, List<Long> candidates, int size) throws TimeSeriesException;
	public void processMatches(Matches request) throws TimeSeriesException;
	public void updatePatternIndex(MasterData md, String idxTableName, long startKey, long endKey, 
			Map<Integer,List<Long>> lookup, int projHashCount, int projHashNumber) throws TimeSeriesException;
	public boolean storePattern(String cluster, String pguid, List<String> pattern) throws TimeSeriesException;
	public List<String> fetchPattern(String cluster, String pguid) throws TimeSeriesException;
	public List<String> fetchCapturedPatterns(String cluster) throws TimeSeriesException;
	
}
