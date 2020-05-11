// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mapdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mapdb.DB;
import org.mortbay.log.Log;

import com.squigglee.coord.interfaces.IConfig;
import com.squigglee.coord.interfaces.ITask;
import com.squigglee.coord.interfaces.IndexingTask;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesConfig;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.CommandType;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.sketch.LocalitySensitiveHasher;

public class MasterDataHandlerImpl extends SchemaHandlerImpl implements IMasterDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mapdb.MasterDataHandlerImpl");
	
	@Override
	public MasterData getMasterData(int ln, String guid, long startts) throws TimeSeriesException {
		
		IConfig configurationService = serviceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(this.clusterName, ln, guid, startts, -1);
		configurationService.close();
		if (list != null && list.size() > 0)
			return list.get(0);
		else
			return null;
	}

	@Override
	public List<TimeSeriesConfig> getMasterData(int ln, String guid)
			throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(this.clusterName, ln, guid, -1, -1);
		configurationService.close();
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		TimeSeriesConfig config = null;
		DateTime storagestart = null;
		DateTime storageend = null;
		
		for (MasterData md : list) {		
			storagestart = new DateTime( md.getStartts(), DateTimeZone.UTC);
			storageend = TimeSeriesShard.getMaxEndDate(md.getFreq(), storagestart);
			config = new TimeSeriesConfig(guid, md.getLn(), md.getFreq(), md.getDatatype(),
				md.getIndexes(), storagestart, storageend);
			configs.add(config);
		}
		if (configs == null || configs.isEmpty())
			Log.debug("No master data configured for id =" + guid + " and ln = " + ln);
		return configs;
	}

	@Override
	public List<TimeSeriesConfig> getNodeMasterData(int ln)
			throws TimeSeriesException {
		
		IConfig configurationService = serviceFactory.getConfigurationService();
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		Map<String,List<TimeSeriesConfig>> vals = configurationService.getConfig(this.clusterName, ln);
		for (String guid : vals.keySet())
			configs.addAll(vals.get(guid));
		configurationService.close();
		if (configs.isEmpty())
			Log.debug("No master data configured for ln = " + ln);
		return TimeSeriesConfig.collapseTimeIntervals(configs);
	}
	
	@Override
	public List<MasterData> getMasterData(long id) throws TimeSeriesException {
		List<MasterData> list = new ArrayList<MasterData>();
		IConfig configurationService = serviceFactory.getConfigurationService();
		list.add(configurationService.getMasterData(this.clusterName, id));
		configurationService.close();
		return list;
	}

	@Override
	public List<MasterData> getMasterData(int ln, String guid, long startts, long endts) throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(this.clusterName, ln, guid, startts, endts);
		configurationService.close();
		return list;
	}

	@Override
	public int[] createMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		ITask taskService = serviceFactory.getTaskService();
		//get the old configuration get the prior configured indexes
		List<MasterData> list = getMasterData(config.getLogicalNode(), config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis());
		String oldIndexes = null;
		if (list != null && list.size() > 0)
			oldIndexes = list.get(0).getIndexes();
		
		int[] result = configurationService.createConfig(this.clusterName, config);
		rebalanceIndexes(oldIndexes, config, taskService);
		
		taskService.close();
		configurationService.close();
		
		return result;
	}

	@Override
	public List<MasterData> getMasterDataForBlocks(int ln, String guid, long startts, long endts) throws TimeSeriesException {
		return getMasterData(ln, guid, startts, endts);
	}

	@Override
	public int[] updateMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		//no difference with Cassandra or zookeeper for insert vs. update 
		return createMasterData(config);
	}

	@Override
	public int[] deleteMasterData(TimeSeriesConfig config)
			throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		ITask taskService = serviceFactory.getTaskService();
		String oldIndexes = config.getIndexes();
		TimeSeriesConfig deleteConfig = new TimeSeriesConfig(config.getGuid(), config.getLogicalNode(), 
				config.getFrequency(), config.getDatatype(), null, config.getStartDate(), config.getEndDate());
		rebalanceIndexes(oldIndexes, deleteConfig, taskService);
		int[] result = configurationService.deleteConfig(this.clusterName, config);
		taskService.close();
		configurationService.close();
		return result;
	}

	@Override
	public int deletePatternIndexTables(List<MasterData> list, String index) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return 0;
    	
		int successCount = 0;
		for (MasterData md : list) {
			int projections = LocalitySensitiveHasher.getConfiguredIndexProjections(index);
			for ( int proj = 0; proj < projections; proj++) {
				String tableName = index + "_" + md.getId() + "_" + proj;
				delete(md.getId(), tableName);
			}
		}
		return successCount;
	}
	
	@Override
	public int[] getDataStatus(long id, String dataType) throws TimeSeriesException {
		//DB db = getDatabase("" + id);
		ConcurrentNavigableMap<Object,Object> table = getTable("" + id, dataType);
		
		int lastoffset = -1;
		if (table != null && !table.descendingKeySet().isEmpty())
			lastoffset = Integer.parseInt(table.descendingKeySet().first().toString());
		int firstoffset = -1;
		if (table != null && !table.keySet().isEmpty())
			firstoffset = Integer.parseInt(table.keySet().first().toString());
		
		//s.close();
		//db.close();
		return new int[]{firstoffset,lastoffset};
	}
	
	private void rebalanceIndexes(String oldIndexes, TimeSeriesConfig config, ITask service) throws TimeSeriesException {
		List<String> added = getIndexDiffs(oldIndexes, config.getIndexes());
		List<String> dropped = getIndexDiffs(config.getIndexes(), oldIndexes); 
		List<MasterData> list = getMasterData(config.getLogicalNode(), config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis());
		
		for (String index : dropped) {
			deletePatternIndexTables(list, index);
		}
		
		for (String index : added) {
			String[] tokens = index.split("_"); 
			for (MasterData md : list) {
				Map<IndexType,Integer> maxDims = md.getMaxIndexDimension();
				IndexType it = IndexType.valueOf(tokens[0]);
				int dataChunk = 0;
				if (it.equals(IndexType.ptrn))
					dataChunk = LocalNodeProperties.getIndexChunkSize();
				else
					dataChunk = LocalNodeProperties.getSketchChunkSize();
				int[] dataStatus = getDataStatus(md.getId(), md.getDatatype());
				if ( (dataStatus[1] == -1) 
						|| (dataStatus[0] == -1)
						|| ( (dataStatus[1] - dataStatus[0]) < maxDims.get(it)) )
					continue;
				
				int	nchunks = (int) Math.ceil((dataStatus[1] - dataStatus[0] + 1)*1.0/dataChunk);
				for (int i = 0; i < nchunks; i++) {
					int st = (i*dataChunk - maxDims.get(it));
					if (st < 0)
						st = 0;
					int et = ( ((i+1)*dataChunk -1) + maxDims.get(it));
					if (et > dataStatus[1])
						et = dataStatus[1];
					if (et > TsrConstants.COLUMN_FAMILY_MAX_COLUMNS)
						et = TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
					if (it.equals(IndexType.ptrn)) {
						for (int destinationLn : service.getReplicaSet(this.clusterName, md.getId())) {
							service.addTask(new IndexingTask(this.clusterName, destinationLn, md.getId(), st, et, index, CommandType.INSERT));
						}
					} else 
						service.addTask(new IndexingTask(this.clusterName, md.getLn(), md.getId(), st, et, index, CommandType.INSERT));
				}	
			}
		}
	}
	
	private List<String> getIndexDiffs(String oldIndexes, String newIndexes) {
		List<String> added = new ArrayList<String>();
		if (newIndexes != null && newIndexes.length() > 0) {
			if (oldIndexes == null || oldIndexes.length() == 0) {
				for (String index : newIndexes.split(";")) {
					added.add(index); 
				}
			} else {
				for (String index : newIndexes.split(";"))
					if (!oldIndexes.contains(index))
						added.add(index);
			}
		}
		return added;
	}

}
