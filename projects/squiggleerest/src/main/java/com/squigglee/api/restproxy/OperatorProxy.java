package com.squigglee.api.restproxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.Operators;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.operators.FunctionParser;

public class OperatorProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.TimeSeriesProxy");
	protected int localLn = 0;
	protected int limit = 0;
	protected String localCluster = null;
	protected IDataHandler dataHandler = null;
	
	public OperatorProxy(IDataHandler dataHandler, int localLn, String localCluster, int limit) {
		this.dataHandler = dataHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
		this.limit = limit;
	}
	
	//TODO proxy to the nearest location with the greatest number of variables datalocal 
	public Operators fetchComputedTimeSeries(Operators request) {
		try {
			List<SortedMap<Long, Object>> results = new ArrayList<SortedMap<Long,Object>>();
			for (int i=0; i< request.getFuncs().size(); i++)
				results.add(new TreeMap<Long,Object>());
			//List<String> translatedFunctions = new ArrayList<String>();
			Map<String, SortedMap<Long, Object>> operatorData = new HashMap<String, SortedMap<Long, Object>>();
			Map<String, SortedMap<Integer, List<String>>> dataSources = FunctionParser.parseVariables(new ArrayList<String>(request.getVars().keySet()));
			for (String cluster : dataSources.keySet()) {
				for (int ln : dataSources.get(cluster).keySet()) {
					for (String parm : dataSources.get(cluster).get(ln)) {
						SortedMap<Long, Object> fetchedData = new TreeMap<Long, Object>();
						List<MasterData> mdList = dataHandler.getMasterData(cluster, ln, parm, request.getStart(), request.getEnd());
						
						//proxy this call
						for (MasterData md : mdList) {
							if (dataHandler.getReplicaSet(md.getCluster(), md.getLn()).contains(localLn)) {
							fetchedData.putAll(dataHandler.fetchTimeSeries(md, TimeSeriesShard.getOffset(md.getFreq(), request.getStart(), request.getStartHfOffset()), 
									TimeSeriesShard.getOffset(md.getFreq(), request.getEnd(), request.getEndHfOffset())));
							} else {
								NodeStatus alternateLocation = dataHandler.getAlternateLocation(md.getCluster(), md.getLn());
								logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
								TimeSeries proxyTs = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).getTimeSeriesJSON
										(new TimeSeries(md.getCluster(), md.getLn(), md.getGuid(), request.getStart(), 
												request.getStartHfOffset(), request.getEnd(), request.getEndHfOffset()));
								System.out.println("Executed proxy query for cluster = " + md.getCluster() + " ln = " + md.getLn() + " id = " + md.getGuid() + " start = " 
										+ request.getStart() + " hfoffset = " + request.getStartHfOffset() + " and end = " + request.getEnd() 
										+ " and hfoffset = " + request.getEndHfOffset());
								logger.debug("Executed proxy query for cluster = " + md.getCluster() + " ln = " + md.getLn() + " id = " + md.getGuid() + " start = " 
										+ request.getStart() + " hfoffset = " + request.getStartHfOffset() + " and end = " + request.getEnd() 
										+ " and hfoffset = " + request.getEndHfOffset());
								
								fetchedData.putAll(proxyTs.getData());
							}
							
						}
						operatorData.put(request.getVars().get("'" + cluster + "#" + ln + "#" + parm+ "'"), fetchedData);
					}
				}
			}
			for (int i =0; i< request.getFuncs().size(); i++) {
				ExpressionBuilder builder = new ExpressionBuilder(request.getFuncs().get(i));
				for (String var : operatorData.keySet())
					builder.variable(var);
				Expression exp = builder.build();
				for (int l = 0; l < (request.getEnd() - request.getStart() + 1); l++) {
					for (String var : operatorData.keySet()) {
						exp.setVariable(var, Double.parseDouble(operatorData.get(var).get(new Long(l)).toString()));
					}
					results.get(i).put(new Long(l),exp.evaluate());
				}
			}
			request.setComputedResults(results);
			request.setErrorMessage(null);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching computed time series for funcs = " + request.getFuncs() + " and vars = " + request.getVars(), e);
			request.setErrorMessage(e.getMessage());
		}
		return request;
	}
}
