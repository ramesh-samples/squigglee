// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.jobs.indexing;

import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.sketch.CountExact;
import com.squigglee.core.sketch.CountMin;
import com.squigglee.core.sketch.ISketch;
import com.squigglee.core.interfaces.HandlerFactory;

public class SketchIndexingTask  implements Runnable {
	private static Logger logger = Logger.getLogger("com.squigglee.jobs.indexing.SketchIndexingTask");
	protected IndexingTask request = null;
	protected Map<Integer,IndexingTask> runningTasks = null;
	protected int seq = 0;
	protected MasterData md = null;
	protected String wguid = null;
	protected int dataChunk = 0;
	protected int maxChunks = 1;
	protected ISketch sketch = null;
	
	public SketchIndexingTask (int seq, IndexingTask request, MasterData md, Map<Integer,IndexingTask> runningTasks, 
			String wguid, int dataChunk, int maxChunks) throws TimeSeriesException {
		this.request = request;
		this.runningTasks = runningTasks;
		this.seq = seq;
		this.md = md;
		this.wguid = wguid;
		this.dataChunk = dataChunk;
		this.maxChunks = maxChunks;
		initialize();
	}

	@Override
	public void run() {
		try {
			updateIndex();
			ITaskService taskService = ServiceFactory.getTaskService();
			if (runningTasks.containsKey(seq))
				runningTasks.remove(seq);
			taskService.deleteAssignedTask(request, wguid);
			logger.info("Completed sketch indexing task " + request);
		} catch (TimeSeriesException tse) {
			logger.error("Failed to run sketch index task for sketch = " + request.getIndexName(),tse);
		}
	}
	
	private void initialize() throws TimeSeriesException {
		if (request.getIndexName().contains("CM"))
			sketch = new CountMin();
		else if (request.getIndexName().contains("EX"))
			sketch = new CountExact();
		
		System.out.println("Received indexing request = " + request);
		logger.debug("Received indexing request = " + request);
		sketch.loadSerializedIndex(request.getCluster(), request.getId(), request.getIndexName(), HandlerFactory.getSketchHandler());
	}
	
	private boolean updateIndex() throws TimeSeriesException {

		if (!HandlerFactory.getSketchHandler().getConfigurationStatus(md.getCluster(), md.getId(), request.getIndexName())) {
			return true;	//request can be safely deleted 
		}
		
		long startOffset = -1;
		long endOffset = -1;
		int chunkSize = dataChunk>10000?10000:dataChunk;
		int	chunks = (int) Math.ceil((request.getEndoffset() - request.getStartoffset() + 1)*1.0/chunkSize);
		if (chunks > maxChunks)
			chunks = maxChunks;
		for ( int i = 0; i < chunks; i++) {
			
			startOffset = request.getStartoffset() + i*chunkSize;
			endOffset = request.getStartoffset() + (i+1)*chunkSize -1 ;
			
			if (endOffset > request.getEndoffset())
				endOffset = request.getEndoffset();
			
			boolean result = false;
			if (request.getDataOperation().equals(CommandType.INSERT) || request.getDataOperation().equals(CommandType.UPDATE))
				result = doInserts(md, (int) startOffset, (int) endOffset);
			else
				result = doDeletes(md, (int) startOffset, (int) endOffset, request.getPriorData());
			
			sketch.updateIndex(md.getCluster(), md.getId(), HandlerFactory.getSketchHandler(), false);	//always call with false since sketch must be already created
			if (result) {
				logger.debug("Updated sketch (chunk #" + i + ") for request " + request);
				System.out.println("Updated sketch (chunk #" + i + ") for request " + request);
			}
		}
		return true;	//request is successfully completed 
	}
	
	private boolean doInserts(MasterData md, int start, int end) throws TimeSeriesException {
		System.out.println("Inserting to sketch for master data " + md);
		Map<Long, Object> timeSeries = null;
		timeSeries = HandlerFactory.getDataHandler().fetchTimeSeries(md, start, end);
		if (request.getDataOperation().equals(CommandType.INSERT) || request.getDataOperation().equals(CommandType.UPDATE)) {
			if (timeSeries.isEmpty()) {
				logger.error("Sketching request for inserted data but data is not yet available, keeping task open for subsequent re-execution for request " + request);
				return false;	//request is kept open until data is available, for how long? TBD
			}
		}
			
		for (Object offset : timeSeries.keySet()) {
			if (timeSeries.get(offset) != null)
				sketch.update(((Long) offset).intValue(), Double.parseDouble(timeSeries.get(offset).toString()));
		}
		
		return true;
	}
	
	private boolean doDeletes(MasterData md, int start, int end, byte[] priorData) throws TimeSeriesException {
		System.out.println("Deleting from sketch for master data " + md);
		if (priorData == null)
			return false;
		if (sketch.statistics() != null && sketch.statistics().getCount() == 0)			//sketch is already empty, no reason to delete
			return false;
		
		SortedMap<Long, Object> deletedTimeSeries = HandlerFactory.getDataHandler().fetchTimeSeries(priorData);
		//for (Integer offset : deletedTimeSeries.keySet()) {
		//	System.out.println("Deleted value for offset = " + offset + " has value = " + deletedTimeSeries.get(offset));
		//}
		if (deletedTimeSeries == null || deletedTimeSeries.isEmpty()) {
			logger.info("No time series data found in serialized prior data for deletes");
			return false;
		}
		for (long offset : deletedTimeSeries.keySet())
			sketch.reverseUpdate(offset, Double.parseDouble(deletedTimeSeries.get(offset).toString()));
		
		return true;
	}
}
