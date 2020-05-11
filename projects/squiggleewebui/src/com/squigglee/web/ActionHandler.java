// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.joda.time.DateTime;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.api.restproxy.ConfigurationProxy;
import com.squigglee.api.restproxy.PatternProxy;
import com.squigglee.api.restproxy.SynopsesProxy;
import com.squigglee.api.restproxy.TimeSeriesProxy;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.entity.ViewType;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.interfaces.HandlerFactory;

public class ActionHandler {

	protected static HandlerFactory factory = null;
	
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String localCluster = null;
	protected static int limit = 10000;
	protected static TimeSeriesProxy tsProxy = null;
	protected static PatternProxy patternProxy = null;
	protected static SynopsesProxy synopsesProxy = null;
	protected static ConfigurationProxy configProxy = null;
	
	protected DistanceMeasure measure = new EuclideanDistance();
	
	static {
		RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
	}
	
	public ActionHandler() throws TimeSeriesException {
		localLn = LocalNodeProperties.getNodeLogicalNumber();
		localCluster = LocalNodeProperties.getClusterName();
		replicaOf = LocalNodeProperties.isReplicaOf();
		limit = LocalNodeProperties.getSqlQueryLimit();
		tsProxy = new TimeSeriesProxy(HandlerFactory.getDataHandler(), localLn, localCluster, limit);
		patternProxy = new PatternProxy(HandlerFactory.getMasterDataHandler(), HandlerFactory.getPatternHandler(), localLn, localCluster);
		synopsesProxy = new SynopsesProxy(HandlerFactory.getMasterDataHandler(), HandlerFactory.getSampledDataHandler(), 
				HandlerFactory.getSketchHandler(), localLn, localCluster, limit);
		configProxy = new ConfigurationProxy(HandlerFactory.getMasterDataHandler(), localLn, localCluster);
	}
	
	public void handle(WebJson webJson) throws TimeSeriesException, NumberFormatException, ParseException {
		if (webJson.getCommon() != null)
			handle(webJson.getCommon());
		else if (webJson.getConfigure() != null)
			handle(webJson.getConfigure());
		else if (webJson.getOperate() != null)
			handle(webJson.getOperate());
		else if (webJson.getMatch() != null)
			handle(webJson.getMatch());
		else if (webJson.getSummarize() != null)
			handle(webJson.getSummarize());
	}
	
	public void handle(CommonPageJson commonPageJson) throws TimeSeriesException {
		IStatusHandler statusHandler = HandlerFactory.getStatusHandler();
		if (LocalNodeProperties.isBoostrapNode())
			commonPageJson.setIsBoot("true");
		else
			commonPageJson.setIsBoot("false");
		
		if (commonPageJson.getAction().equals(WebAction.ST_UPDATE))
			commonPageJson.setClusterStatus(statusHandler.fetchClusterStatus(localCluster));
	}
	
	public void handle(OperatePageJson operatePageJson) throws TimeSeriesException {
		if (operatePageJson.getAction().equals(WebAction.OP_ADD)) {
			int dataNode = findDataNodeInTopology(operatePageJson.getClusterStatus());
			for (NodeStatus server : operatePageJson.getClusterStatus()) {
				HandlerFactory.getMasterDataHandler().updateNode(server.getCluster(), server.getLogicalNumber(), "CREATE", server.getDataCenter(), "TBD", "TimeSeriesNode_" + 
						server.getLogicalNumber(), server.isBootstrapNode(), server.isSeedNode(), 
    					dataNode, server.getStorage(), server.getStype());
			}
		}
		
		if (operatePageJson.getAction().equals(WebAction.OP_DELETE)) {
			for (NodeStatus server : operatePageJson.getClusterStatus()) {
				HandlerFactory.getMasterDataHandler().updateNode(server.getCluster(), server.getLogicalNumber(), server.getAddress(), server.getDataCenter(), server.getInstanceId(), "DELETE", 
						server.isBootstrapNode(), server.isSeedNode(), server.getReplicaOf(), server.getStorage(), server.getStype());
			}
		}
		
		if (operatePageJson.getAction().equals(WebAction.OP_RESTART)) {
			for (NodeStatus server : operatePageJson.getClusterStatus()) {
				HandlerFactory.getMasterDataHandler().updateNode(server.getCluster(), server.getLogicalNumber(), server.getAddress(), server.getDataCenter(), server.getInstanceId(), "RESTART", 
						server.isBootstrapNode(), server.isSeedNode(), server.getReplicaOf(), server.getStorage(), server.getStype());
			}
		}
		if (operatePageJson.getAction().equals(WebAction.OP_REFRESH)) {
			for (NodeStatus server : operatePageJson.getClusterStatus()) {
				HandlerFactory.getMasterDataHandler().updateNode(server.getCluster(), server.getLogicalNumber(), server.getAddress(), server.getDataCenter(), server.getInstanceId(), "REFRESH", 
						server.isBootstrapNode(), server.isSeedNode(), server.getReplicaOf(), server.getStorage(), server.getStype());
			}
		}
	}

	public void handle(ConfigurePageJson configurePageJson) throws TimeSeriesException {

		if (configurePageJson.getAction().equals(WebAction.CF_SET)) {
			cf_setConfig(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_ADD)) {
			cf_addConfig(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_REMOVE)) {
			cf_removeConfig(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_ADD_INDEX)) {
			cf_addIndex(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_DROP_INDEX)) {
			cf_dropIndex(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_ADD_SKETCH)) {
			cf_addSketch(configurePageJson);
		} 
		else if (configurePageJson.getAction().equals(WebAction.CF_DROP_SKETCH)) {
			cf_dropSketch(configurePageJson);
		}
	}
	
	public void handle(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException, ParseException {
		if (sketchPageJson.getAction().equals(WebAction.SK_UPDATE_STATS))
			spUpdateStatistics(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_POINT_QUERY))
			spPointQuery(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_RANGE_QUERY))
			spRangeQuery(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_UPDATE_HISTOGRAM))
			spHistogram(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_UPDATE_SAMPLING_HISTOGRAM))
			spSamplingHistogram(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_CONFIGURE))
			spHandleConfigure(sketchPageJson);
		else if (sketchPageJson.getAction().equals(WebAction.SK_INVERSE_QUERY))
			spInverseQuery(sketchPageJson);
	}
	
	public void handle(MatchPageJson matchPageJson) throws NumberFormatException, TimeSeriesException, ParseException {
		if (matchPageJson.getAction().equals(WebAction.DR_UPDATE_TS))
			handleUpdateTs(matchPageJson);
		else if (matchPageJson.getAction().equals(WebAction.DR_FETCH_AHEAD))
			handleFetchAhead(matchPageJson);
		else if (matchPageJson.getAction().equals(WebAction.DR_CAPTURE))
			handleCapture(matchPageJson);
		else if (matchPageJson.getAction().equals(WebAction.DR_MATCH))
			handleMatch(matchPageJson);
		else if (matchPageJson.getAction().equals(WebAction.DR_FETCH_PATTERN))
			handlePattern(matchPageJson);
		else if (matchPageJson.getAction().equals(WebAction.DR_CONFIGURE))
			handleConfigure(matchPageJson);
	}
	
	private void cf_setConfig(ConfigurePageJson configurePageJson) throws NumberFormatException, TimeSeriesException {
		List<ConfigData> configList = getConfigData(configurePageJson.getCluster(), Integer.parseInt(configurePageJson.getLn()));
		configurePageJson.setNodeConfig(configList);
		configurePageJson.setSampleStartOfToday(DateTimeHelper.getSampleStartOfToday());
		configurePageJson.setSampleEndOfToday(DateTimeHelper.getSampleEndOfToday());
	}
	
	private void cf_addConfig(ConfigurePageJson configurePageJson) throws TimeSeriesException {
		String error = "";
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			DateTime start =  DateTimeHelper.parseDateString(cd.getStart());
			DateTime end =  DateTimeHelper.parseDateString(cd.getEnd());
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf(cd.getFrequency()), cd.getDatatype(), cd.getIndexes(), start, end);
			System.out.println("Adding data configuration for " + tsc);
			//mdHandler.reset(cd.getDatatype());
			//List<Long> result = mdHandler.createMasterData(tsc);
			TimeSeriesConfig result = configProxy.createConfig(tsc);
			if (result.getErrorMessage() != null)
				error += "Failed to create master data for config " + tsc + "--" + result.getErrorMessage() + ";";
		}
		if (error.length() > 0) {
			configurePageJson.setError(error);
			configurePageJson.setMessage("FAIL");
		} else {
			configurePageJson.setMessage("OK");
		}
		cf_setConfig(configurePageJson);
	}

	private void cf_removeConfig(ConfigurePageJson configurePageJson) throws TimeSeriesException {
		String error = "";
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			DateTime start =  DateTimeHelper.parseDateString(cd.getStart());
			DateTime end =  DateTimeHelper.parseDateString(cd.getEnd());
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf(cd.getFrequency()), cd.getDatatype(), cd.getIndexes(), start, end);
			//System.out.println("Removing data configuration for cluster " + cd.getCluster() + " ln = " + cd.getLn() + " for guid = " + cd.getParameter() + " indexes " + cd.getIndexes());
			System.out.println("Removing data configuration for " + tsc);
			
			//first delete any indexes or sketches associated with this configuration
			//if (cd.getIndexes() != null && cd.getIndexes().length() > 0) {
			//	cf_dropIndex(cd);
			//	cf_dropSketch(cd);
			//}
			
			TimeSeriesConfig result = configProxy.deleteConfig(tsc);
			if (result.getErrorMessage() != null)
				error += "Failed to delete master data for config " + tsc + "--" + result.getErrorMessage() + ";";
			//mdHandler.reset(cd.getDatatype());
			//System.out.println("About to delete master data for time series config = " + ts);
			//List<Long> result = mdHandler.deleteMasterData(ts);
			
			
			//if (result.size() == 0)
			//	error += "Failed to delete master data for parameter " + cd.getParameter() + ";";
		}
		if (error.length() > 0) {
			configurePageJson.setError(error);
			configurePageJson.setMessage("FAIL");
		} else {
			configurePageJson.setMessage("OK");
		}
		cf_setConfig(configurePageJson);
	}

	private void cf_addIndex(ConfigurePageJson configurePageJson) throws NumberFormatException, TimeSeriesException {
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			//start & end dates, frequency, datatype do not matter for index updates
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf("MILLIS"), null, cd.getIndexes(), DateTime.now(), DateTime.now()); 
			configProxy.addIndex(tsc);
			//mdHandler.updateIndex(cd.getCluster(), Integer.parseInt(configurePageJson.getLn()), cd.getParameter(), cd.getIndexes(), false);
		}
		cf_setConfig(configurePageJson);
	}

	private void cf_dropIndex(ConfigurePageJson configurePageJson) throws NumberFormatException, TimeSeriesException {
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			//start & end dates, frequency, datatype do not matter for index updates
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf("MILLIS"), null, cd.getIndexes(), DateTime.now(), DateTime.now()); 
			configProxy.dropIndex(tsc);
			//mdHandler.updateIndex(cd.getCluster(), Integer.parseInt(configurePageJson.getLn()), cd.getParameter(), cd.getIndexes(), true);
		}
		cf_setConfig(configurePageJson);
	}
	
	//private void cf_dropIndex(ConfigData cd) throws NumberFormatException, TimeSeriesException {
	//	mdHandler.updateIndex(cd.getCluster(), Integer.parseInt(cd.getLn()), cd.getParameter(), cd.getIndexes(), true);
	//}

	private void cf_addSketch(ConfigurePageJson configurePageJson) throws NumberFormatException, TimeSeriesException {
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			//start & end dates, frequency, datatype do not matter for index updates
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf("MILLIS"), null, cd.getIndexes(), DateTime.now(), DateTime.now()); 
			configProxy.addIndex(tsc);
			//mdHandler.updateIndex(cd.getCluster(), Integer.parseInt(configurePageJson.getLn()), cd.getParameter(), cd.getIndexes(), false);
		}
		cf_setConfig(configurePageJson);
	}

	private void cf_dropSketch(ConfigurePageJson configurePageJson) throws NumberFormatException, TimeSeriesException {
		for (ConfigData cd : configurePageJson.getNodeConfig()) {
			//start & end dates, frequency, datatype do not matter for index updates
			TimeSeriesConfig tsc = new TimeSeriesConfig(cd.getCluster(), cd.getParameter(), Integer.parseInt(configurePageJson.getLn()), 
					Frequency.valueOf("MILLIS"), null, cd.getIndexes(), DateTime.now(), DateTime.now()); 
			configProxy.dropIndex(tsc);
			//mdHandler.updateIndex(cd.getCluster(), Integer.parseInt(configurePageJson.getLn()), cd.getParameter(), cd.getIndexes(), true);
		}
		cf_setConfig(configurePageJson);
	}

	//private void cf_dropSketch(ConfigData cd) throws NumberFormatException, TimeSeriesException {
	//	indexHandler.updateIndex(cd.getCluster(), Integer.parseInt(cd.getLn()), cd.getParameter(), cd.getIndexes(), true);
	//}
	
	private void spHandleConfigure(SketchPageJson sketchPageJson) throws TimeSeriesException {
		List<ConfigData> configList = getConfigData(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()));
		sketchPageJson.setNodeConfig(configList);
	}
	
	private void spSamplingHistogram(SketchPageJson sketchPageJson) throws TimeSeriesException, ParseException {
		HandlerFactory.getSketchHandler().reset(sketchPageJson.getDataType());
		HandlerFactory.getSampledDataHandler().reset(sketchPageJson.getDataType());
		
		SortedMap<Integer, Long> histogram = HandlerFactory.getSketchHandler().getSketchHistogram(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getLn()), 
				sketchPageJson.getSamplingHistogramRequest().getParameter(), Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getBins()));
		sketchPageJson.getSpHistogram().clear();
		for (Integer i : histogram.keySet())
			sketchPageJson.getSpHistogram().add(new HistBar(i,histogram.get(i)));
		DateTime start =  DateTimeHelper.parseDateString(sketchPageJson.getSamplingHistogramRequest().getStart());
		DateTime end =  DateTimeHelper.parseDateString(sketchPageJson.getSamplingHistogramRequest().getEnd());
		
		SortedMap<Integer,Long> sampledDataHistogram = synopsesProxy.getSampledDataHistogram(sketchPageJson.getCluster(),
				Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getLn()), 
				sketchPageJson.getSamplingHistogramRequest().getParameter(),  
				start.getMillis(), 
				(sketchPageJson.getSamplingHistogramRequest().getStartHfOffset()==null?0:Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getStartHfOffset())), 
				end.getMillis(), 
				(sketchPageJson.getSamplingHistogramRequest().getEndHfOffset()==null?0:Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getEndHfOffset())),
				Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getBins()),
				Integer.parseInt(sketchPageJson.getSamplingHistogramRequest().getSampleSize()));
		
		sketchPageJson.getSampledDataHistogram().clear();
		for (Integer i : sampledDataHistogram.keySet())
			sketchPageJson.getSampledDataHistogram().add(new HistBar(i,sampledDataHistogram.get(i)));

	}
	
	private void spHistogram(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException {
		//SortedMap<Integer, Long> histogram = HandlerFactory.getSketchHandler().getSketchHistogram(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), 
		//		sketchPageJson.getSpParameter(), Integer.parseInt(sketchPageJson.getBins()));
		SortedMap<Integer, Long> histogram = synopsesProxy.getSketchHistogram(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), 
				sketchPageJson.getSpParameter(), Integer.parseInt(sketchPageJson.getBins()));
		sketchPageJson.getSpHistogram().clear();
		for (Integer i : histogram.keySet())
			sketchPageJson.getSpHistogram().add(new HistBar(i,histogram.get(i)));
	}	
	
	private void spPointQuery(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException {
		//long pointQueryResult = HandlerFactory.getSketchHandler().pointQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), sketchPageJson.getSpPointQuery());
		long pointQueryResult = synopsesProxy.pointQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), sketchPageJson.getSpPointQuery());
		sketchPageJson.setSpPointResult("" + pointQueryResult);
	}
	
	private void spRangeQuery(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException {
		//long rangeQueryResult = HandlerFactory.getSketchHandler().rangeQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), 
		//		sketchPageJson.getSpRangeQuery1(), sketchPageJson.getSpRangeQuery2());
		long rangeQueryResult = synopsesProxy.rangeQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), 
				sketchPageJson.getSpRangeQuery1(), sketchPageJson.getSpRangeQuery2());
		sketchPageJson.setSpRangeResult("" + rangeQueryResult);
	}
	
	private void spInverseQuery(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException {
		double quantile = Double.parseDouble(sketchPageJson.getSpInverseQuery()) / 100.0;
		//Object inverseResult = HandlerFactory.getSketchHandler().inverseQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), quantile);
		Object inverseResult = synopsesProxy.inverseQuery(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter(), quantile + "");
		sketchPageJson.setSpInverseResult(inverseResult.toString());
	}
	
	private void spUpdateStatistics(SketchPageJson sketchPageJson) throws NumberFormatException, TimeSeriesException {
		View sketchView = DynamicTypeTranslator.getViewName(sketchPageJson.getDataType(), ViewType.SKETCH);
		String dataType = DynamicTypeTranslator.getViewDataType(sketchView);
		//TODO proxy this to get distributed results 
		//Stats stats = HandlerFactory.getSketchHandler().statistics(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter());
		Stats stats = synopsesProxy.statistics(sketchPageJson.getCluster(), Integer.parseInt(sketchPageJson.getLn()), sketchPageJson.getSpParameter());
		sketchPageJson.setSpMin("" + stats.getMin());
		sketchPageJson.setSpMax("" + stats.getMax());
		sketchPageJson.setSpCount("" + stats.getCount());
		List<String> heavyHitters = new ArrayList<String>();
		Map<Long, Object> hh = stats.getHeavyHitters();
		if (hh != null) {
			for (Long o : hh.keySet())
				heavyHitters.add(DynamicTypeTranslator.convertDoubleToNumeric((Double) hh.get(o), dataType).toString());
		}
		sketchPageJson.setSpTopkValues(heavyHitters);
		sketchPageJson.setSpFirst("" + stats.getFirst());
		sketchPageJson.setSpLast("" + stats.getLast());
	}
	
	private void handleUpdateTs(MatchPageJson matchPageJson) throws TimeSeriesException, ParseException {
		
		long viewStartts = Long.parseLong(matchPageJson.getTsViewStartts());
		long viewEndts = Long.parseLong(matchPageJson.getTsViewEndts());
		long viewStartHfOffset = Long.parseLong(matchPageJson.getTsViewStartHfOffset());
		long viewEndHfOffset = Long.parseLong(matchPageJson.getTsViewEndHfOffset());
		Frequency currentFrequency = Frequency.valueOf(matchPageJson.getFrequency());
		if (viewStartts < 0)
			viewStartts = DateTimeHelper.parseDateString(matchPageJson.getTsPanelStart()).getMillis();
		if (viewEndts < 0)
			viewEndts = DateTimeHelper.parseDateString(matchPageJson.getTsPanelEnd()).getMillis();
		if (viewStartHfOffset < 0)
			viewStartHfOffset = 0;
		if (currentFrequency.equals(Frequency.MICROS))
				if (viewEndHfOffset < 0)
					viewEndHfOffset = 999;
		if (currentFrequency.equals(Frequency.NANOS))
			if (viewEndHfOffset < 0)
				viewEndHfOffset = 999999;

		int windowSize = Integer.parseInt(matchPageJson.getTsWindowSize());
		boolean last = Boolean.parseBoolean(matchPageJson.getLast());
		
		//System.out.println("***Ln=" + matchPageJson.getLn() + " and ID=" + matchPageJson.getTsPanelParameter() + " start view ts = " + viewStartts + ", and end view ts = " + viewEndts + ", and point count = " + windowSize);
		List<MasterData> mdListForTs = HandlerFactory.getMasterDataHandler().getMasterData(matchPageJson.getCluster(), Integer.parseInt(matchPageJson.getLn()), matchPageJson.getTsPanelParameter(), 
				viewStartts, viewEndts);

		if (mdListForTs == null || mdListForTs.size() == 0 || mdListForTs.get(0).getDatatype().equalsIgnoreCase("blob") 
				|| mdListForTs.get(0).getDatatype().equalsIgnoreCase("text") || mdListForTs.get(0).getDatatype().equalsIgnoreCase("string")) {
			matchPageJson.setError("No data available or wrong data type for charting");
			return;
		}
		SortedMap<Long, Object> results = new TreeMap<Long, Object>();
		HandlerFactory.getDataHandler().reset(mdListForTs.get(0).getDatatype());
		SortedMap<Long, MasterData> orderedList = null;
		if (last)
			orderedList = new TreeMap<Long, MasterData>(Collections.reverseOrder()); //process last master data fetched first
		else
			orderedList = new TreeMap<Long, MasterData>(); //process first master data fetched first
		SortedMap<Long, Map<Long, Object>> queriedResults = new TreeMap<Long, Map<Long,Object>>(Collections.reverseOrder()); //process last master data fetch first 
		for (MasterData md: mdListForTs)
			orderedList.put(md.getId(), md);
		
		for (Long ts : orderedList.keySet()) {
			MasterData md = orderedList.get(ts);

			
			SortedMap<Long, Object> mdResult = new TreeMap<Long,Object>();
			long startOffset, endOffset;
			if (currentFrequency.equals(Frequency.MICROS)) {
				endOffset = (new DateTime(viewEndts)).getMillisOfSecond()*1000 + viewEndHfOffset;
				if (endOffset > 999999)
					endOffset = 999999;
				startOffset = viewStartHfOffset; 
				if (startOffset < 0 ) 
					startOffset = 0;
			} else if (currentFrequency.equals(Frequency.NANOS)) {
				endOffset = (new DateTime(viewEndts)).getMillisOfSecond()*1000000 + viewEndHfOffset;
				if (endOffset > 999999999)
					endOffset = 999999999;
				startOffset = viewStartHfOffset; 
				if (startOffset < 0 ) 
					startOffset = 0;
			} else {
				endOffset = (viewEndts - md.getStartts());
				if (endOffset > TsrConstants.COLUMN_FAMILY_MAX_COLUMNS)
					endOffset = TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
				startOffset = (int) (viewStartts - md.getStartts()); 
				if (startOffset < 0 ) 
					startOffset = 0;
			}
			
			System.out.println("Making proxy request for data id = " + md.getId() + " guid = " + md.getGuid() + " startts = " + viewStartts +
					" start hf offset " + viewStartHfOffset + " endts " + viewEndts + " end hf offset " + viewEndHfOffset +
					" for " + (last?"last ":"first ") + windowSize + " values " + " for ln = " + md.getLn() + " in cluster " + md.getCluster());
			mdResult = tsProxy.getData(md.getCluster(), md.getLn(), md.getGuid(), viewStartts, (int) viewStartHfOffset, viewEndts
					, (int) viewEndHfOffset, windowSize, last);
			
			System.out.println("start view offset = " + startOffset + ", and end view offset = " + endOffset + " and fetched results = " + mdResult.size());			
			queriedResults.put(md.getStartts(), mdResult);
			long count = 0;
			for (Map<Long, Object> mdr : queriedResults.values())
				count += mdr.size();
			if (count >= windowSize)
				break;
		}
		
		for (Long st : queriedResults.keySet()) {
			Map<Long, Object> mdResult = queriedResults.get(st);
			ArrayList<Long> keys = new ArrayList<Long>(mdResult.keySet());
			for (int i = keys.size()-1; i >= 0; i--) {
				results.put( new Long(st + keys.get(i)), mdResult.get(keys.get(i)));
			}
			if (results.size() >= windowSize)
				break;
		}

		Timeseries ts = new Timeseries();
		ts.setStartts("" + viewStartts);
		ts.setFreq(mdListForTs.get(0).getFreq().toString());
		for (Long i : results.keySet()) {
			ts.getData().add(new Datapair((new Long(i - viewStartts)).toString(), results.get(i).toString()));
		}
		
		if (ts.getData().size() > 0) {
			if (currentFrequency.equals(Frequency.MICROS) || currentFrequency.equals(Frequency.NANOS)) {
				matchPageJson.setTsViewStartHfOffset(ts.getData().get(0).getOffset());
				matchPageJson.setTsViewEndHfOffset(ts.getData().get(ts.getData().size()-1).getOffset());
				matchPageJson.setTsViewStartts(viewStartts + "");
				matchPageJson.setTsViewEndts(viewEndts + "");
			} else {
				matchPageJson.setTsViewStartts( (viewStartts + Long.parseLong(ts.getData().get(0).getOffset()) ) + "");
				matchPageJson.setTsViewEndts( (viewStartts + Long.parseLong(ts.getData().get(ts.getData().size()-1).getOffset()) ) + "");
			}
		}
		matchPageJson.setTsResults(ts);
	}
	
	private void handleFetchAhead(MatchPageJson matchPageJson) throws TimeSeriesException, ParseException {
		handleUpdateTs(matchPageJson); 		
	}
	
	private void handleCapture(MatchPageJson matchPageJson) throws TimeSeriesException {
		HandlerFactory.getPatternHandler().reset(matchPageJson.getDataType());
		boolean result = false;
		
		List<String> patternList = new ArrayList<String>();
		for ( int i = 0; i< matchPageJson.getPattern().size(); i++)
			patternList.add(matchPageJson.getPattern().get(i).toString());
		result = HandlerFactory.getPatternHandler().storePattern(matchPageJson.getCluster(), matchPageJson.getTsPanelPatternName(), patternList);
		if (result)
			matchPageJson.setMessage("OK");
		else
			matchPageJson.setMessage("FAIL");
	}
	
	private void handlePattern(MatchPageJson matchPageJson) throws TimeSeriesException {
		if (matchPageJson.getPtrnPanelPatternName() != null && matchPageJson.getPtrnPanelPatternName().length() > 0) {
			HandlerFactory.getPatternHandler().reset(matchPageJson.getDataType());
			List<String> fetchedPatternList = HandlerFactory.getPatternHandler().fetchPattern(matchPageJson.getCluster(), matchPageJson.getPtrnPanelPatternName());
			matchPageJson.getPattern().clear();
			for (int i=0; i< fetchedPatternList.size(); i++)
				matchPageJson.getPattern().add(fetchedPatternList.get(i).toString());
		}
	}	
	
	private void handleMatch(MatchPageJson matchPageJson) throws TimeSeriesException {
		List<TimeSeriesConfig> idList = new ArrayList<TimeSeriesConfig>();
		String dataType = null;
		if (matchPageJson.getSearchSelections().size() == 0)
			return;
		else
			dataType = matchPageJson.getSearchSelections().get(0).getDatatype();
		for (ConfigData cd : matchPageJson.getSearchSelections()) {
			idList.add(new TimeSeriesConfig(cd.getCluster(), cd.getParameter(),Integer.parseInt(cd.getLn()),Frequency.values()[0],cd.getDatatype(),"",
					DateTimeHelper.parseDateString(cd.getStart()),
					DateTimeHelper.parseDateString(cd.getEnd())));
		}
		
		HandlerFactory.getPatternHandler().reset(dataType);
		if (matchPageJson.getPattern() == null || matchPageJson.getPattern().size() == 0)
			return;
		double[] ptrn = new double[matchPageJson.getPattern().size()];
		for ( int i=0; i< ptrn.length; i++)
			ptrn[i] = Double.parseDouble(matchPageJson.getPattern().get(i));
		double radius = Double.parseDouble(matchPageJson.getMatchDistance());
		int matchCount = Integer.parseInt(matchPageJson.getMatchCount());
		System.out.println("Issuing match query for pattern " + matchPageJson.getPattern());
		Matches request = new Matches();
		request.setPattern(new com.squigglee.core.entity.Pattern( matchPageJson.getPattern()));
		request.setRadius(radius);
		request.setTopk(matchCount);
		request.setRequestDomain(idList);
		
		PatternProxy patternProxy = new PatternProxy(HandlerFactory.getMasterDataHandler(), HandlerFactory.getPatternHandler(), localLn, localCluster);
		request = patternProxy.executePatternMatchSplits(request);
		
		int count = 0;
		for (double dist : request.getMatchResults().keySet())
			for (com.squigglee.core.entity.Match m : request.getMatchResults().get(dist)) {
				count++;
				Match match = new Match();
				match.setMatchLogicalNumber("" + m.getLn());
				match.setMatchParameter(m.getId());
				match.setMatchStart("" + m.getValues().firstKey());
				for (Object o : m.getValues().values())
					match.getResults().add(o.toString());
				match.setActualDistance("" + m.getDistance());
				matchPageJson.getMatches().add(match);
			}
		System.out.println("Found " + count + " matches for pattern " + matchPageJson.getPattern());
	}
	
	private void handleConfigure(MatchPageJson matchPageJson) throws TimeSeriesException {
		List<ConfigData> configList = getConfigData(matchPageJson.getCluster(), Integer.parseInt(matchPageJson.getLn()));
		matchPageJson.setNodeConfig(configList);
		List<String> patternDataTypes = new ArrayList<String>();
		for (ConfigData cd : configList) {
			if (!patternDataTypes.contains(cd.getDatatype()))
				patternDataTypes.add(cd.getDatatype());
		}
		for (String viewDataType : patternDataTypes) {
			View view = DynamicTypeTranslator.getViewName(viewDataType, ViewType.PATTERN);
			if (view.equals(View.PATTERNINTEGERS))
				matchPageJson.getCapturedPatterns().setIntPatterns(HandlerFactory.getPatternHandler().fetchCapturedPatterns(matchPageJson.getCluster()));
			if (view.equals(View.PATTERNDOUBLES))
				matchPageJson.getCapturedPatterns().setDoublePatterns(HandlerFactory.getPatternHandler().fetchCapturedPatterns(matchPageJson.getCluster()));
			if (view.equals(View.PATTERNFLOATS))
				matchPageJson.getCapturedPatterns().setFloatPatterns(HandlerFactory.getPatternHandler().fetchCapturedPatterns(matchPageJson.getCluster()));
			if (view.equals(View.PATTERNLONGS))
				matchPageJson.getCapturedPatterns().setLongPatterns(HandlerFactory.getPatternHandler().fetchCapturedPatterns(matchPageJson.getCluster()));
			if (view.equals(View.PATTERNBOOLEANS))
				matchPageJson.getCapturedPatterns().setBooleanPatterns(HandlerFactory.getPatternHandler().fetchCapturedPatterns(matchPageJson.getCluster()));
		}
	}

	private int findDataNodeInTopology(List<NodeStatus> nodes) throws TimeSeriesException {
		int dataNode = -1;
		for (NodeStatus server : nodes) {
			if (server.getLogicalNumber() == server.getReplicaOf()) {
				if (dataNode >=0)
					throw new TimeSeriesException("Multiple data nodes cannot be specified in a single topology");
				dataNode = server.getLogicalNumber();
			}
		}
		if (dataNode < 0)
			throw new TimeSeriesException("A single data node must be listed in each requested topology");
		return dataNode;
	}

	private List<ConfigData> getConfigData(String cluster, int ln) throws NumberFormatException, TimeSeriesException {
		List<TimeSeriesConfig> list = HandlerFactory.getMasterDataHandler().getNodeMasterData(cluster, ln);
		List<ConfigData> configList = new ArrayList<ConfigData>();
		ConfigData cd = null;
		for (TimeSeriesConfig tsc : list) {
			cd = new ConfigData();
			cd.setParameter(tsc.getGuid());
			cd.setDatatype(tsc.getDatatype());
			cd.setFrequency(tsc.getFrequency() + "");
			cd.setStart(DateTimeHelper.getDateString(tsc.getStartDate()));
			cd.setStartts("" + tsc.getStartDate().getMillis());
			cd.setEnd(DateTimeHelper.getDateString(tsc.getEndDate()));
			cd.setEndts("" + tsc.getEndDate().getMillis());
			cd.setIndexes(tsc.getIndexes());
			int ptrnSize = patternIndexSize(tsc.getIndexes());
			if (ptrnSize > 0) {
				cd.setPatternIndexSize("" + ptrnSize);
				cd.setIsPatternIndexed("true");
			} else
				cd.setIsPatternIndexed("false");
			if (isSketched(tsc.getIndexes()))
				cd.setIsSketched("true");
			else
				cd.setIsSketched("false");
			cd.setCluster(tsc.getCluster());
			cd.setLn("" + tsc.getLogicalNode());
			configList.add(cd);
		}
		return configList;
	}
		
	private boolean isSketched(String indexString) {
		if (indexString == null || indexString.length() == 0)
			return false;
		String[] indexes = indexString.split(";");
		for (String idx : indexes) {
			if ( idx.toLowerCase().startsWith(IndexType.skchCM.name().toLowerCase()) || idx.toLowerCase().startsWith(IndexType.skchEX.name().toLowerCase()) ) {
				return true;
			}
		}
		return false;
	}
	
	private int patternIndexSize(String indexString) {
		if (indexString == null || indexString.length() == 0)
			return -1;
		String[] indexes = indexString.split(";");
		 
			for (String idx : indexes) {
				if (idx.toLowerCase().startsWith(IndexType.ptrn.name().toLowerCase())) {
					String[] parsedPtrnIndex = idx.split("_");
					return Integer.parseInt(parsedPtrnIndex[1]);
				}
			}
		return -1;
	}
}

