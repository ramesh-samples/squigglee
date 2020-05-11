package com.squigglee.api.restproxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IPatternHandler;

public class PatternProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.PatternProxy");
	protected static ExecutorService executorService = null;
	protected IMasterDataHandler mdHandler = null;
	protected IPatternHandler patternHandler = null;
	protected String localCluster = null;
	protected int localLn = 0;
	
	public PatternProxy(IMasterDataHandler mdHandler, IPatternHandler patternHandler, int localLn, String localCluster) {
		this.mdHandler = mdHandler;
		this.patternHandler = patternHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
	}
	
	public Matches executePatternMatchSplits(Matches request) {
		try {
			if (!request.isDataLocal())	{
				Map<String,Map<Integer,Matches>> splits = getSplits(request);
				int count = getSplitCount(splits);
				System.out.println(count + " splits found for pattern = " + request.getPattern());
				executorService = Executors.newFixedThreadPool(count);
				CountDownLatch cdLatch = new CountDownLatch(count);
				int counter = 0;
				request.getMatchResults().clear();
				for (String cluster : splits.keySet()) {
					for (int ln : splits.get(cluster).keySet()) {
						counter++;
						if (cluster.equalsIgnoreCase(localCluster) && ln == localLn) {
							System.out.println("Local split found for ln = " + ln + " and cluster = " + cluster);
							cdLatch.countDown();
							request.setDataLocal(true);
							patternHandler.processMatches(request);
						}
						else
							executorService.execute(getTask(counter, cluster, ln, splits.get(cluster).get(ln), request, cdLatch));
					}
				}
				try {
					cdLatch.await();
				} catch (InterruptedException e) {
					logger.error("Error waiting for all threads to complete executing proxy pattern match requests " + splits);
				}
			} else {
				patternHandler.processMatches(request);
			}
			request.setErrorMessage(null);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching matches or request " + request, e);
			request.setErrorMessage(e.getMessage());
		}
		return request;
	}
	
	private Map<String,Map<Integer,Matches>> getSplits(Matches request) throws TimeSeriesException {
		Map<String,Map<Integer,Matches>> splits = 
				new HashMap<String, Map<Integer, Matches>>();
		
		for (TimeSeriesConfig tsc : request.getRequestDomain()) {
			if (!splits.containsKey(tsc.getCluster()))
				splits.put(tsc.getCluster(), new HashMap<Integer, Matches>());
			
			List<Integer> replicaSet = mdHandler.getReplicaSet(tsc.getCluster(), tsc.getLogicalNode());
			if (replicaSet == null)
				continue;
			for (int ln : replicaSet) {
				TimeSeriesConfig replicaConfig = tsc.clone();
				replicaConfig.setLogicalNode(ln);
				replicaConfig.setDataLocal(true);
				if (!splits.get(tsc.getCluster()).containsKey(ln)) {
					Matches nodeRequest = new Matches(request.getPattern(), new ArrayList<TimeSeriesConfig>(), request.getRadius(), request.getTopk(), true);
					splits.get(tsc.getCluster()).put(ln, nodeRequest);
				}
				splits.get(tsc.getCluster()).get(ln).getRequestDomain().add(replicaConfig);
			}
		}
		for (String cluster : splits.keySet())
			for (int ln : splits.get(cluster).keySet())
				System.out.println("Split for node " + ln + " in cluster " + cluster + " = " + splits.get(cluster).get(ln).getRequestDomain());
		
		return splits;
	}
	
	private int getSplitCount(Map<String,Map<Integer,Matches>> splits) {
		int count = 0;
		for (String cluster : splits.keySet()) {
			count += splits.get(cluster).size();
		}
		return count;
	}
	
	private Runnable getTask(int tid, String cluster, int ln, Matches request, Matches original, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			private String cluster = null;
			private int ln = 0;
			private Matches request = null;
			private Matches original = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				System.out.println("Launching remote proxy pattern match request for thread # " + tid 
						+ " for cluster = " + cluster + " and ln = " + ln);
				try {
					NodeStatus nodeLocation = mdHandler.getLocation(cluster, ln);
					logger.debug("Location for remote data = " + nodeLocation.getAddress() + " for thread # " + tid);
					Matches result = RESTFactory.getPatternProxy(nodeLocation.getAddress()).fetchMatchesJSON(request);
					original.addMatches(result.getMatchResults());
				} catch (Exception e) {
					logger.error("Found error executing proxy pattern match request for thread # " + tid 
							+ " for cluster = " + cluster + " and ln = " + ln, e);
				} finally {
					cdLatch.countDown();
				}
			}
			public Runnable initialize(int tid, String cluster, int ln, Matches request, Matches original, CountDownLatch cdLatch) {
				this.tid = tid;
				this.cluster = cluster;
				this.ln = ln;
				this.request = request;
				this.original = original;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, cluster, ln, request, original, cdLatch);
	}
}
