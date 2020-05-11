package com.squigglee.api.rest.impl;

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.ISynopsesRESTService;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class SynopsesRESTService extends RestBase implements
		ISynopsesRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.SynopsesRESTService");
	
	@Override
	public Long pointQuery(String cluster, int ln, String guid, String val) {
		try {
			return HandlerFactory.getSketchHandler().pointQuery(cluster, ln, guid, val);
		} catch (TimeSeriesException e) {
			logger.error("Error fetching point query for cluster = " + cluster + " ln = " + ln + " id = " + guid + " value = " + val,e);
		}
		return 0L;
	}

	@Override
	public Long rangeQuery(String cluster, int ln, String guid,
			String startVal, String endVal) {
		try {
			return HandlerFactory.getSketchHandler().rangeQuery(cluster, ln, guid, startVal, endVal);
		} catch (TimeSeriesException e) {
			logger.error("Error fetching range query for cluster = " + cluster + " ln = " + ln + " id = " 
				+ guid + " startVal = " + startVal + " endVal = " + endVal,e);
		}
		return 0L;
	}

	@Override
	public String inverseQuery(String cluster, int ln, String guid, String quantile) {
		try {
			return synopsesProxy.inverseQuery(cluster, ln, guid, quantile);
		} catch (TimeSeriesException e) {
			logger.error("Error fetching inverse query for cluster = " + cluster + " ln = " + ln + " id = " 
				+ guid + " and quantile = " + quantile, e);
		}
		return null;
	}

	@Override
	public SortedMap<Integer, Long> getSketchHistogram(String cluster, int ln, String guid, int bins) {
		try {
			return synopsesProxy.getSketchHistogram(cluster, ln, guid, bins);
		} catch (TimeSeriesException e) {
			logger.error("Error fetching getSketchHistogram for cluster = " + cluster + " ln = " + ln + " id = " + guid + " ", e);
		}
		return new TreeMap<Integer,Long>();
	}

	@Override
	public Stats statistics(String cluster, int ln, String guid) {
		try {
			return synopsesProxy.statistics(cluster, ln, guid);
		} catch (TimeSeriesException e) {
			logger.error("Error fetching point query for cluster = " + cluster + " ln = " + ln + " id = " + guid, e);
		}
		return null;
	}

	@Override
	public SortedMap<Integer, Long> getSampledDataHistogram(String cluster,	int ln, String id, long start, int startHfOffset, 
			long end, int endHfOffset, int bins, int sampleSize) {
		logger.debug("Received request for sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
				+ start + " and end = " + end);
		try {
			return synopsesProxy.getSampledDataHistogram(cluster, ln, id, start, startHfOffset, end, endHfOffset, bins, sampleSize);
		} catch (TimeSeriesException tse) {
			logger.error("Error fetching sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
					+ start + " and end = " + end, tse);
		}
		return new TreeMap<Integer, Long>();
	}

	@Override
	public TimeSeries getSampledTimeSeriesJSON(String cluster, int ln, String guid, long start, int startHfOffset, long end, int endHfOffset, int sampleSize) {
		logger.debug("Executed local query for sampled time series in cluster " + cluster + " in ln = " + ln + " id = " + guid + " start = " 
					+ start + " hfoffset = " + startHfOffset + " and end = " + end + " and hfoffset = " + endHfOffset + " and sample size = " + sampleSize);
		TimeSeries ts = new TimeSeries(cluster, ln, guid, start, startHfOffset, end, endHfOffset);
		try {
			return synopsesProxy.getSampledTimeSeries(ts, sampleSize);
		} catch (TimeSeriesException tse) {
			logger.error("Error fetching sampled time series in cluster " + cluster + " in ln = " + ln + " id = " + guid + " start = " 
					+ start + " hfoffset = " + startHfOffset + " and end = " + end + " and hfoffset = " + endHfOffset + " and sample size = " + sampleSize);
			ts.setErrorMessage(tse.getMessage());
			return ts;
		}
	}


}
