package com.squigglee.api.rest.impl;

import java.util.SortedMap;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.ITimeSeriesRESTService;
import com.squigglee.api.restproxy.TimeSeriesProxy;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class TimeSeriesRESTService extends RestBase implements ITimeSeriesRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.TimeSeriesRESTService");
	
	@Override
	public TimeSeries getTimeSeriesJSON(TimeSeries ts) {
		logger.debug("Received post fetch request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		System.out.println("Received post fetch request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
			TimeSeriesProxy tsProxy = null;
			try {
				tsProxy = new TimeSeriesProxy(HandlerFactory.getDataHandler(), localLn, localCluster, limit);
			} catch (TimeSeriesException e) {
				ts.setErrorMessage(e.getMessage());
			}
			if (tsProxy != null)
				ts.setData(tsProxy.getData(ts.getCluster(), ts.getLn(),ts.getId(),ts.getStart(),ts.getStartHfOffset(),ts.getEnd(),ts.getEndHfOffset()));
		return ts;
	}
	
	@Override
	public TimeSeries getTimeSeriesJSON(String cluster, int ln, String id, long start, int startHfOffset, long end, int endHfOffset) {
		logger.debug("Received get fetch request for time series for ln = " + ln + " id = " + id + " start = " + start + " end = " + end);
		System.out.println("Received get fetch request for time series for ln = " + ln + " id = " + id + " start = " + start + " end = " + end);
		TimeSeries ts = new TimeSeries();
		ts.setLn(ln);
		ts.setId(id);
		ts.setStart(start);
		ts.setStartHfOffset(startHfOffset);
		ts.setEnd(end);
		ts.setEndHfOffset(endHfOffset);
		
		ts.setData(tsProxy.getData(ts.getCluster(), ts.getLn(),ts.getId(),ts.getStart(),ts.getStartHfOffset(),ts.getEnd(),ts.getEndHfOffset()));
		
		return ts;
	}
	
	@Override
	public TimeSeries updateTimeSeriesJSON(TimeSeries ts) {
		logger.debug("Received update request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		System.out.println("Received update request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		
		ts = tsProxy.putData(ts.getCluster(), ts.getLn(), ts.getId(), ts.getStart(), ts.getStartHfOffset(), ts.getEnd(), ts.getEndHfOffset(), ts.getData());
		return ts;
	}
	
	@Override
	public TimeSeries updateTimeSeriesBulkJSON(TimeSeries ts) {
		logger.debug("Received bulk update for time series for cluster = " + ts.getCluster() + " bulk data base64 size = " + ts.getBulkData().length());
		System.out.println("Received bulk update for time series for cluster = " + ts.getCluster() + " bulk data base64 size = " + ts.getBulkData().length());
		return tsProxy.putData(ts.getCluster(), ts.getLn(), ts.getId(), ts.getStart(), ts.getStartHfOffset(), ts.getEnd(), ts.getEndHfOffset(), ts.getBulkData());
	}

	@Override
	public TimeSeries getTimeSeriesBulkJSON(String cluster, int ln, String id, long start, int startHfOffset, long end, int endHfOffset) {
		logger.debug("Received get request for bulk time series for ln = " + ln + " id = " + id + " start = " + start + " end = " + end);
		System.out.println("Received get request for bulk time series for ln = " + ln + " id = " + id + " start = " + start + " end = " + end);
		TimeSeries ts = new TimeSeries(cluster, ln, id, start, end);
		ts = tsProxy.getBulkData(cluster, ln, id, start, startHfOffset, end, endHfOffset);
		return ts;
	}
	
	@Override
	public TimeSeries getTimeSeriesBulkJSON(TimeSeries ts) {
		logger.debug("Received get request for bulk time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " + ts.getStart() 
				+ " start offset = " + ts.getStartHfOffset() + " end = " + ts.getEnd() + " end offset = " + ts.getEndHfOffset());
		System.out.println("Received get request for bulk time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " + ts.getStart() 
				+ " start offset = " + ts.getStartHfOffset() + " end = " + ts.getEnd() + " end offset = " + ts.getEndHfOffset());
		ts = tsProxy.getBulkData(ts.getCluster(), ts.getLn(), ts.getId(), ts.getStart(), ts.getStartHfOffset(), ts.getEnd(), ts.getEndHfOffset());
		return ts;
	}

	@Override
	public TimeSeries getSequencedTimeSeriesJSON(String cluster, int ln, String id, long start,	int startHfOffset, long end, int endHfOffset, int count, boolean last) {
		logger.debug("Received " + (last?"last":"first") + " sequenced fetch request for " + count + " time series for ln = " + ln + " id = " + id + " start = " + start + " hfoffset = " + startHfOffset + " end = " + end + " hfoffset = " + endHfOffset);
		System.out.println("Received " + (last?"last":"first") + " sequenced fetch request for " + count + " time series for ln = " + ln + " id = " + id + " start = " + start + " hfoffset = " + startHfOffset + " end = " + end + " hfoffset = " + endHfOffset);
		TimeSeries ts = new TimeSeries(cluster, ln, id, start, startHfOffset, end, endHfOffset);
		SortedMap<Long,Object> data = tsProxy.getData(cluster, ln, id, start, startHfOffset, end, endHfOffset, count, last);
		ts.setData(data);
		return ts;
	}
	
	@Override
	public TimeSeries getSequencedTimeSeriesJSON(String cluster, int ln, String id, long startts, int startOffset, int endOffset, int count,  boolean last) {
		logger.debug("Received " + (last?"last":"first") + " sequenced fetch request for " + count + " time series for ln = " + ln + " id = " + id + " startts = " + startts + " startOffset = " + startOffset + " endOffset = " + endOffset);
		System.out.println("Received " + (last?"last":"first") + " sequenced fetch request for " + count + " time series for ln = " + ln + " id = " + id + " startts = " + startts + " startOffset = " + startOffset + " endOffset = " + endOffset);
		return tsProxy.getData(cluster, ln, id, startts, startOffset, endOffset, count, last);
	}
}
