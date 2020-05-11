// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.indexing;

import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.sketch.LocalitySensitiveHasher;
import com.squigglee.core.interfaces.HandlerFactory;

public class PatternIndexingTask implements Runnable {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.indexing.PatternIndexingTask");
	protected IndexingTask request = null;
	protected Map<Integer,IndexingTask> runningTasks = null;
	protected int seq = 0;
	protected MasterData md = null;
	protected String wguid = null;
	protected int dataChunk = 0;
	protected int maxChunks = 1;
	protected int replicaCount = 0;
	protected int replicaNumber = 0;
	protected LocalitySensitiveHasher lsh = null;
	
	public PatternIndexingTask (int seq, IndexingTask request, MasterData md, Map<Integer,IndexingTask> runningTasks, 
			String wguid, int dataChunk, int maxChunks, int replicaCount, int replicaNumber) throws TimeSeriesException {
		this.md = md;
		this.request = request;
		this.runningTasks = runningTasks;
		this.seq = seq;
		this.wguid = wguid;
		this.dataChunk = dataChunk;
		this.maxChunks = maxChunks;
		this.replicaCount = replicaCount;
		this.replicaNumber = replicaNumber;
		initialize();
	}

	public void run() {
		try {
			boolean result = false;
			if (request.getDataOperation().equals(CommandType.DELETE))	//skip the deletes to the pattern indexes
				result = true;
			else
				result = updateIndex();
			if (result) {
				System.out.println("Completed indexing task = " + request);
				logger.info("Completed pattern indexing task = " + request);
			}
			else
			{
				System.out.println("Failed to complete indexing task = " + request);
				logger.debug("Failed to complete indexing task = " + request);
			}
			ITaskService taskService = ServiceFactory.getTaskService();
			taskService.deleteAssignedTask(request, wguid);
			if (runningTasks.containsKey(seq))
				runningTasks.remove(seq);
			//taskService.close();
		} catch (Exception tse) {
			logger.error("Failed to run pattern index task for request = "  + request,tse);
		} finally {
			
		}
	}
	
	private void initialize() throws TimeSeriesException {
		lsh = new LocalitySensitiveHasher(replicaCount, replicaNumber);		
		lsh.loadSerializedIndex(request.getCluster(), request.getId(), request.getIndexName(), HandlerFactory.getIndexHandler());
		System.out.println("Initialized LSH indexes for replicaNumber = " + replicaNumber + " of total replicas = " + replicaCount);
		lsh.getLookupMap().clear();
	}
	
	private boolean updateIndex() throws TimeSeriesException {
		
		if (!(HandlerFactory.getIndexHandler()).getConfigurationStatus(request.getCluster(), request.getId(),request.getIndexName())) {
			return false;
		}
		//no purpose to unrolling pattern index, all retrievals are distance matched
		if (request.getDataOperation().equals(CommandType.DELETE))
			return true;
		
		//long startOffset = request.getStartoffset();
		//long endOffset = request.getEndoffset();
		
		long startOffset = -1;
		long endOffset = -1;
		int chunkSize = dataChunk>10000?10000:dataChunk;
		int	chunks = (int) Math.ceil((request.getEndoffset() - request.getStartoffset() + 1)*1.0/chunkSize);
		if (chunks > maxChunks)
			chunks = maxChunks;
		logger.debug("Started indexing task " + request + " at time = " + DateTime.now());
		for ( int i = 0; i < chunks; i++) {
			SortedMap<Long, Object> timeSeries;
			if (endOffset != -1)
				startOffset = endOffset + 1;
			else
				startOffset = request.getStartoffset() + i*chunkSize;
			endOffset = request.getStartoffset() + (i+1)*chunkSize -1 ;
			if (endOffset > request.getEndoffset())
				endOffset = request.getEndoffset();
			try {
				timeSeries = HandlerFactory.getDataHandler().fetchTimeSeries(md, (int) startOffset, (int) endOffset);
				if (request.getDataOperation().equals(CommandType.INSERT) || request.getDataOperation().equals(CommandType.UPDATE)) {
					if (timeSeries.isEmpty() || timeSeries.size() < lsh.getSize()) {
						System.out.println("No data yet to complete indexing request for Index " + request.getIndexName() + "_" + request.getId() 
								+ " for range [" + startOffset + "," + endOffset + "]");
						logger.debug("No data yet to complete indexing request for Index " + request.getIndexName() + "_" + request.getId() 
								+ " for range [" + startOffset + "," + endOffset + "]");
						return true;
					}
					System.out.println("Size of retrieved time series for pattern indexing = " + timeSeries.size() + " in range [" 
							+ timeSeries.firstKey() + "," + timeSeries.lastKey() + "]");
					logger.debug("Size of retrieved time series for pattern indexing = " + timeSeries.size() + " in range ["
							+ timeSeries.firstKey() + "," + timeSeries.lastKey() + "]");
				}
				lsh.getLookupMap().clear();
				for (Long offset : timeSeries.keySet()) {
					double[] slice = getTimeSlice(timeSeries, offset, lsh.getSize());
					if (slice != null) {
						lsh.index((Long) offset, slice);
						endOffset = (Long) offset;
					}
				}
				lsh.updateIndex(request.getCluster(), request.getId(), HandlerFactory.getPatternHandler());
				System.out.println("Stored " + lsh.getLookupMap().size() + " pattern indexes (chunk #" + (i+1) + ") with range = [" 
						+ timeSeries.firstKey() + "," + endOffset + "] "  + request + " at time " + (new DateTime()));
				logger.debug("Stored " + lsh.getLookupMap().size() + " pattern indexes (chunk #" + (i+1) + ") with range = [" 
						+ timeSeries.firstKey() + "," + endOffset + "] "  + request + " at time " + (new DateTime()));
				
				//System.out.println("Stored pattern index of request "  + request + " at time " + (new DateTime()));
				//logger.debug("Stored pattern index request "  + request + " at time " + (new DateTime()));
			} catch (Exception e) {
				logger.error("Found error updating pattern index "  + request, e);
				System.out.println("Found error updating pattern index "  + request + " -- error message = " + e.getMessage());
				return false;
			}
		}
		logger.debug("Completed indexing task " + request + " at time = " + DateTime.now());
		lsh.getLookupMap().clear();
		return true;
	}
	
	private double[] getTimeSlice(SortedMap<Long,Object> timeSeries, long offset ,int size) {
		double[] ts = new double[size];
		SortedMap<Long,Object> subMap = timeSeries.tailMap(offset);
		if (subMap.size() < size)
			return null;
		int cntr = 0;
		for (Object key : subMap.keySet()) {
			ts[cntr++] = Double.parseDouble(subMap.get(key).toString());
			if (cntr == size)
				break;
		}
		return ts;
	}
}
