// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.ITimeSeriesDeserializer;
import com.squigglee.core.serializers.ITimeSeriesSerializer;

public class DataHandlerMixin  extends MasterDataHandlerMixin {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.DataHandlerImpl");
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	
	public DataHandlerMixin(ITimeSeriesDeserializer deserializer, ITimeSeriesSerializer serializer) {
		this.deserializer = deserializer;
		this.serializer = serializer;
	}
	
	public void postSyncJob(String cluster, long id, long start, long end, CommandType operation, byte[] data) throws TimeSeriesException {
		ICEPService syncService = ServiceFactory.getCEPService();
		SyncTask syncTask = new SyncTask(cluster, id, start, end, operation, data);
		syncService.addSync(syncTask);
	}
	
	public void updataSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		ICEPService syncService = ServiceFactory.getCEPService();
		syncService.logSync(syncTask, LocalNodeProperties.getNodeLogicalNumber());	
	}
	
	public void postIndexingJobs(SyncTask syncTask) throws TimeSeriesException {
		//System.out.println("In postIndexingJobs method");
		ITaskService service = ServiceFactory.getTaskService();
		MasterData md = getMasterData(syncTask.getCluster(), syncTask.getId());
		if (md == null)
			return;
		
		Map<IndexType,Integer> maxDims = md.getMaxIndexDimension();
		if (maxDims.isEmpty())
			return;		// no indexes configured
		for (String index : md.getIndexes().split(";")) {
			String[] tokens = index.split("_"); 
			IndexType it = IndexType.valueOf(tokens[0]);
			long st = (syncTask.getStartoffset() - maxDims.get(it));
			long et = (syncTask.getEndoffset() + maxDims.get(it));
			
			if (it.equals(IndexType.ptrn)) {	//load balance the pattern indexes across the replica set
				//for (int destinationLn : service.getReplicaSet(clusterName, md.getId())) {
				int destinationLn = LocalNodeProperties.getNodeLogicalNumber();	// post indexing jobs only locally
				IndexingTask task = new IndexingTask(syncTask.getCluster(), destinationLn, md.getId(), (st < 0 ? 0 : st), 
						(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, syncTask.getDataOperation());
				if (syncTask.getDataOperation().equals(CommandType.DELETE) || syncTask.getDataOperation().equals(CommandType.UPDATE)) {
					if (syncTask.getData() != null && syncTask.getData().length > 0)
						task.setPriorData(syncTask.getData());
				}
				service.addTask(task);
				//}
			} else if (md.getLn() == LocalNodeProperties.getNodeLogicalNumber()) {	// process sketches only once per replica set 
				IndexingTask task = new IndexingTask(syncTask.getCluster(), md.getLn(), md.getId(), (st < 0 ? 0 : st), 
						(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, syncTask.getDataOperation());
				if (syncTask.getDataOperation().equals(CommandType.DELETE) || syncTask.getDataOperation().equals(CommandType.UPDATE)) {
					if (syncTask.getData() != null && syncTask.getData().length > 0) {
						task.setPriorData(syncTask.getData());
					}
				}
				service.addTask(task);
			}
		}
		//service.close();
	}
	
	public SortedMap<Long, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException {
		SortedMap<Long,Object> output = new TreeMap<Long,Object>();
		if (data == null)
			return output;
		deserializer.setRawData(data);
		int blockCount = deserializer.getBlockCount();
		if (blockCount > 1)
			logger.info("Serialized data has multiple blocks");
		int[] insertCounts = new int[blockCount];
		
		for (int i = 0; i< insertCounts.length; i++) {
				int dataCount = 0;
				long startts = deserializer.getStartts(i);
				MasterData masterData = getMasterData(deserializer.getCluster(i), deserializer.getLn(i),deserializer.getGuid(i),startts);
				if (masterData == null)
					throw new TimeSeriesException("No master data found for serialized data ");
				long initialOffset = 0;
				if (startts > masterData.getStartts())
					initialOffset = TimeSeriesShard.getOffset(masterData.getFreq(), startts);				
				dataCount = deserializer.getDataCount(i);
				for (int j = 0; j< dataCount; j++)
					output.put((initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j));
		}
		return output;
	}

	
	public void deleteNodeData(String cluster, int ln) throws TimeSeriesException {
		IDataService dataService = ServiceFactory.getDataService();
		dataService.deleteNode(cluster, ln);
		//dataService.close();
	}

}
