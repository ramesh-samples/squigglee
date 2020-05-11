package com.squigglee.api.restproxy;

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISketchHandler;

public class SynopsesProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.TimeSeriesProxy");
	protected int localLn = 0;
	protected int limit = 0;
	protected String localCluster = null;
	protected IMasterDataHandler mdHandler = null;
	protected ISampledDataHandler sdHandler = null;
	protected ISketchHandler sketchHandler = null;
	
	public SynopsesProxy(IMasterDataHandler mdHandler, ISampledDataHandler sdHandler, ISketchHandler sketchHandler, int localLn, String localCluster, int limit) {
		this.mdHandler = mdHandler;
		this.sdHandler = sdHandler;
		this.sketchHandler = sketchHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
		this.limit = limit;
	}
	
	public Long pointQuery(String cluster, int ln, String guid, String val) throws TimeSeriesException {
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			return HandlerFactory.getSketchHandler().pointQuery(cluster, ln, guid, val);
		} else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			return RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).pointQuery(cluster, ln, guid, val);
		}
	}
	
	public Long rangeQuery(String cluster, int ln, String guid,	String startVal, String endVal) throws TimeSeriesException {
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			return HandlerFactory.getSketchHandler().rangeQuery(cluster, ln, guid, startVal, endVal);
		} else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			return RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).rangeQuery(cluster, ln, guid, startVal, endVal);
		}
	}
	
	public String inverseQuery(String cluster, int ln, String guid, String quantile) throws NumberFormatException, TimeSeriesException {
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			Object obj = HandlerFactory.getSketchHandler().inverseQuery(cluster, ln, guid, Double.parseDouble(quantile));
			if (obj != null)
				return obj.toString();
		} else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			return RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).inverseQuery(cluster, ln, guid, quantile);
		}
		return null;
	}
	
	public SortedMap<Integer, Long> getSketchHistogram(String cluster, int ln, String guid, int bins) throws TimeSeriesException {
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			return HandlerFactory.getSketchHandler().getSketchHistogram(cluster, ln, guid, bins);
		} else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			return RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).getSketchHistogram(cluster, ln, guid, bins);
		}
	}
	
	public Stats statistics(String cluster, int ln, String guid) throws TimeSeriesException {
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			return HandlerFactory.getSketchHandler().statistics(cluster, ln, guid);
		} else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			return RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).statistics(cluster, ln, guid);
		}
	}
	
	public SortedMap<Integer, Long> getSampledDataHistogram(String cluster,	int ln, String id, long start, int startHfOffset, 
			long end, int endHfOffset, int bins, int sampleSize) throws TimeSeriesException {
		SortedMap<Integer, Long> map = new TreeMap<Integer,Long>();
		if (mdHandler.getReplicaSet(cluster, ln).contains(localLn)) {
			map =  sdHandler.getSampledDataHistogram(cluster, ln, id, bins, start, startHfOffset, end, endHfOffset, sampleSize, sketchHandler);
			System.out.println("Executed local query for sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
					+ start + " and end = " + end);
			logger.debug("Executed proxy local for sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
					+ start + " and end = " + end);
		}
		else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(cluster, ln);
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			map = RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).getSampledDataHistogram(cluster, ln, id, start, startHfOffset, end, endHfOffset, bins, sampleSize);
			System.out.println("Executed proxy query for sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
					+ start + " and end = " + end);
			logger.debug("Executed proxy query for sampled data histogram in cluster " + cluster + " in ln = " + ln + " id = " + id + " start = " 
					+ start + " and end = " + end);
		}
		return map;
	}
	
	public TimeSeries getSampledTimeSeries(TimeSeries ts, int sampleSize) throws TimeSeriesException {
		if (mdHandler.getReplicaSet(ts.getCluster(), ts.getLn()).contains(localLn)) {
			ts = sdHandler.readBlockData(ts.getCluster(), ts.getLn(), ts.getId(), ts.getStart(), ts.getStartHfOffset(), 
					ts.getEnd(), ts.getEndHfOffset(), sampleSize);
			System.out.println("Executed local query for sampled time series in cluster " + ts.getCluster() + " in ln = " + ts.getLn() 
					+ " id = " + ts.getId() + " start = " + ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " 
					+ ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
			logger.debug("Executed local query for sampled time series in cluster " + ts.getCluster() + " in ln = " + ts.getLn() 
					+ " id = " + ts.getId() + " start = " + ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " 
					+ ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		}
		else {
			NodeStatus alternateLocation = mdHandler.getAlternateLocation(ts.getCluster(), ts.getLn());
			logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
			ts = RESTFactory.getSynopsesProxy(alternateLocation.getAddress()).getSampledTimeSeriesJSON(ts.getCluster(), ts.getLn(), 
					ts.getId(), ts.getStart(), ts.getStartHfOffset(), ts.getEnd(), ts.getEndHfOffset(), sampleSize);
			System.out.println("Executed proxy query for sampled time series in cluster " + ts.getCluster() + " in ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
					+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
			logger.debug("Executed proxy query for sampled time series in cluster " + ts.getCluster() + " in ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
					+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		}
		return ts;
	}
}
