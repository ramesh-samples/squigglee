package com.squigglee.coord.interfaces;

import java.util.List;

import com.squigglee.core.entity.TimeSeriesException;

public interface IPatternService  extends ICoordService {
	public boolean storePattern(String cluster, String pguid, List<String> pattern) throws TimeSeriesException;
	public List<String> fetchPattern(String cluster, String pguid) throws TimeSeriesException;
	public List<String> fetchCapturedPatterns(String cluster) throws TimeSeriesException;

}
