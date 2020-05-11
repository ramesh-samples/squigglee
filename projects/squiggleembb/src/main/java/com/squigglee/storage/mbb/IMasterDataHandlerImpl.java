// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.coord.storage.MasterDataHandlerMixin;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IMasterDataHandler;

public class IMasterDataHandlerImpl extends ISchemaHandlerImpl implements IMasterDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.IMasterDataHandlerImpl");
	private MasterDataHandlerMixin mdhMixin = null;
	
	@Override
	public void initialize() {
		super.initialize();
		mdhMixin = new MasterDataHandlerMixin();
	}
	
	@Override
	public MasterData getMasterData(String cluster, int ln, String guid, long startts) throws TimeSeriesException {
		return mdhMixin.getMasterData(cluster, ln, guid, startts);
	}

	@Override
	public List<TimeSeriesConfig> getMasterData(String cluster, int ln, String guid)
			throws TimeSeriesException {
		return mdhMixin.getMasterData(cluster, ln, guid);
	}

	@Override
	public List<TimeSeriesConfig> getMasterDataConfig(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException {
		return mdhMixin.getMasterDataConfig(cluster, ln, guid, startts, endts);	
	}
	
	@Override
	public List<TimeSeriesConfig> getNodeMasterData(String cluster, int ln)
			throws TimeSeriesException {
		return mdhMixin.getNodeMasterData(cluster, ln);
	}
	
	@Override
	public List<TimeSeriesConfig> getGlobalConfig() throws TimeSeriesException {
		Map<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>> globalConfig = mdhMixin.getConfig();
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		for (String cluster : globalConfig.keySet())
			for (int ln : globalConfig.get(cluster).keySet())
				for (String guid : globalConfig.get(cluster).get(ln).keySet())
					configs.addAll(globalConfig.get(cluster).get(ln).get(guid));
		return configs;
	}
	
	@Override
	public List<TimeSeriesConfig> getClusterConfig(String cluster) throws TimeSeriesException {
		Map<Integer, Map<String,List<TimeSeriesConfig>>> clusterConfig = mdhMixin.getConfig(cluster);
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		for (int ln : clusterConfig.keySet())
			for (String guid : clusterConfig.get(ln).keySet())
				configs.addAll(clusterConfig.get(ln).get(guid));
		return configs;
	}
	
	@Override
	public MasterData getMasterData(String cluster, long id) throws TimeSeriesException {
		return mdhMixin.getMasterData(cluster, id);
	}

	@Override
	public List<MasterData> getMasterData(String cluster, List<Long> ids) throws TimeSeriesException {
		return mdhMixin.getMasterData(cluster, ids);
	}
	
	@Override
	public List<MasterData> getMasterData(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException {
		return mdhMixin.getMasterData(cluster, ln, guid, startts, endts);
	}
	
	@Override
	public List<Long> getMasterDataIds(String cluster, int ln, List<String> guids) throws TimeSeriesException {
		return mdhMixin.getMasterDataIds(cluster, ln, guids);
	}
	
	@Override
	public void updateDataStatus(MasterData md, long min, long max) throws TimeSeriesException {
		mdhMixin.updateDataStatus(md, min, max);
	}

	@Override
	public List<Long> createMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		
		if (config.isRollup()) {
			String originalGuid = config.getGuid().substring(0,config.getGuid().lastIndexOf("_"));
			String appendedFrequency = config.getGuid().substring(config.getGuid().lastIndexOf("_") + 1, config.getGuid().length());
			if (!appendedFrequency.equalsIgnoreCase(config.getFrequency().toString()))
				throw new TimeSeriesException("Incorrect configuration rollups must be named with the original time series name appended by the rollup frequency separated by underscore -- " + config);
			List<TimeSeriesConfig> rawConfig = getMasterDataConfig(config.getCluster(), config.getLogicalNode(), originalGuid, 
					config.getStartDate().getMillis(), config.getEndDate().getMillis());
			if (rawConfig == null || rawConfig.isEmpty())
				throw new TimeSeriesException("Original time series must be configured prior to configurting rollups -- " + config);
			if (!TimeSeriesShard.getRollupFrequencies(rawConfig.get(0).getFrequency()).contains(config.getFrequency()))
				throw new TimeSeriesException("Rollup frequencies cannot be finer than the raw frequency -- " + config);
		}
		
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		ITaskService taskService = ServiceFactory.getTaskService();
		System.out.println("Creating master data for config request " + config);
		//get the old configuration get the prior configured indexes
		List<MasterData> list = getMasterData(config.getCluster(), config.getLogicalNode(), config.getGuid(), 
				config.getStartDate().getMillis(), config.getEndDate().getMillis());
		System.out.println("Current master data list = " + list);
		String oldIndexes = null;
		if (list != null && list.size() > 0)
			oldIndexes = list.get(0).getIndexes();
		
		List<Long> result = new ArrayList<Long>();
		if (isDataNode())
			result = configurationService.createConfig(config);	// only update the configuration from data node 
		System.out.println("Created master data ids " + result);
		for (long id : result) {
			MasterData md = configurationService.getMasterData(config.getCluster(), id);
			if (md != null)
				createSchema(md);  // updates the schema at all nodes in replica set
		}

		//also re-verify schema in case this is not a data node
		for (MasterData md: list) {
			createSchema(configurationService.getMasterData(config.getCluster(), md.getId()));  // updates the schema at all nodes in replica set
		}
		
		rebalanceIndexes(oldIndexes, config, taskService);	// updates the indexes at all nodes in replica set 
		
		return result;
	}

	@Override
	public List<MasterData> getMasterDataForBlocks(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException {
		return getMasterData(cluster, ln, guid, startts, endts);
	}

	@Override
	public List<Long> updateMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		return createMasterData(config);
	}
	
	@Override
	public List<Integer> getReplicaSet(String cluster, int dataln) throws TimeSeriesException {
		return mdhMixin.getReplicaSet(cluster, dataln);
	}

	@Override
	public List<Long> deleteMasterData(TimeSeriesConfig config)
			throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		ITaskService taskService = ServiceFactory.getTaskService();
		String oldIndexes = config.getIndexes();
		TimeSeriesConfig deleteConfig = new TimeSeriesConfig(config.getCluster(), config.getGuid(), config.getLogicalNode(), 
				config.getFrequency(), config.getDatatype(), null, config.getStartDate(), config.getEndDate());
		List<MasterData> mdList = getMasterData(config.getCluster(), config.getLogicalNode(), config.getGuid(), 
				config.getStartDate().getMillis(), config.getEndDate().getMillis());
		rebalanceIndexes(oldIndexes, deleteConfig, taskService);
		List<Long> result = new ArrayList<Long>();
		if (isDataNode())
			result = configurationService.deleteConfig(config); // only update the configuration from data node
		
		for (MasterData md : mdList) {
			deleteSchema(config.getCluster(), md.getId());  // updates the schema at all nodes in replica set
		}
		return result;
	}
	
	@Override
	public long[] getDataStatus(MasterData md) {
		try {
			return mdhMixin.getDataStatus(md);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching data status " + md);
			return new long[]{TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, -1};
		}
	}
	
	@Override
	public NodeStatus getAlternateLocation(String cluster, int dataln) throws TimeSeriesException {
		return mdhMixin.getAlternateLocation(cluster, dataln, LocalNodeProperties.getLocalDataCenter());
	}
	
	@Override
	public NodeStatus getLocation(String cluster, int nodeln) throws TimeSeriesException {
		return mdhMixin.getLocation(cluster, nodeln);
	}
	
	@Override
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork() throws TimeSeriesException {
		return mdhMixin.getOverlayNetwork();
	}
	
	@Override
	public void setupCluster(String cluster) throws TimeSeriesException {
		mdhMixin.setupCluster(cluster);
	}
	
	@Override
	public void updateNode(String cluster, int ln, String addr, String dataCenter, String instanceId, String name, boolean isBoot, boolean isSeed, 
			int replicaOf, int storage, String stype) throws TimeSeriesException {
		mdhMixin.updateNode(cluster, ln, addr, dataCenter, instanceId, name, isBoot, isSeed, replicaOf, storage, stype);
	}
	
	@Override
	public int updateIndex(String cluster, int ln, String guid, String indexes, boolean drop) throws TimeSeriesException {
		System.out.println("Received request to " + (drop?"drop":"update") + " indexes " + indexes + " for guid = " + guid + " for node = " + ln + " in cluster " + cluster);
		if (indexes == null || indexes.length() == 0)
			return 0;
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		IIndexService indexService = ServiceFactory.getIndexService();
		//IndexSchemaProxy schemaProxy = new IndexSchemaProxy(this, localln, localCluster);
		int result = 0;
		List<MasterData> list = configurationService.getConfiguredMasterData(cluster, ln, guid, -1, -1);
		if (list == null || list.size() == 0)
			return 0;
	
		String currentIndex = list.get(0).getIndexes();
		
		for (String index : indexes.split(";")) {
			if (drop) {
				if (currentIndex == null || currentIndex.length() == 0)
					break;
				if (currentIndex.contains(index))
					currentIndex = currentIndex.replace(index, "");
				if (currentIndex.contains(";;"))
					currentIndex = currentIndex.replaceAll(";;", ";");
				if (currentIndex.endsWith(";"))
					currentIndex = currentIndex.substring(0,currentIndex.length()-1);
				if (currentIndex.startsWith(";"))
					currentIndex = currentIndex.substring(1,currentIndex.length());
				
				if (isDataNode())
					deleteIndexRecords(list, index, indexService);
				
				if (index.toLowerCase().contains("ptrn")) {
					List<Long> idList = new ArrayList<Long>();
					for (MasterData md : list)
						idList.add(md.getId());
					
					deletePatternIndexTables(idList,index);	
				}
			}
			else {
				if (currentIndex == null || currentIndex.length() == 0)
					currentIndex = index;
				else {
					if (!currentIndex.contains(index)) {
						currentIndex += ";" + index;						
					}
				}
				for (MasterData md : list) {
					if (index.toLowerCase().contains("ptrn"))
						createPatternIndexTables(md.getCluster(), md.getId(), index, TsrConstants.KEYSPACE_PREFIX + TsrConstants.KEYSPACE_SEPARATOR 
								+ md.getLn() + TsrConstants.KEYSPACE_SEPARATOR + md.getDatatype());
				}
			}
		}
		List<MasterData> updatedList = new ArrayList<MasterData>();
		for (MasterData md : list) {
			updatedList.add(new MasterData(md.getCluster(), md.getId(), md.getLn(), md.getGuid(), md.getStartts(), md.getFreq(), 
			md.getDatatype(), currentIndex));
		}
		if (isDataNode()) {
			result = configurationService.updateMasterData(updatedList);
			result = list.size();
		}
		return result;
	}
	
	private int deleteIndexRecords(List<MasterData> list, String index, IIndexService service) throws TimeSeriesException {
		return mdhMixin.deleteIndexRecords(list, index, service);
	}
	
	protected void rebalanceIndexes(String oldIndexes, TimeSeriesConfig config, ITaskService service) throws TimeSeriesException {
		List<String> added = getIndexDiffs(oldIndexes, config.getIndexes());
		List<String> dropped = getIndexDiffs(config.getIndexes(), oldIndexes); 
		
		for (String addition : added)
			updateIndex(config.getCluster(), config.getLogicalNode(), config.getGuid(), addition, false);
		
		for (String deletion : dropped)
			updateIndex(config.getCluster(), config.getLogicalNode(), config.getGuid(), deletion, true);

		if (!isDataNode())
			return;
		
		List<MasterData> list = getMasterData(config.getCluster(), config.getLogicalNode(), config.getGuid(), 
				config.getStartDate().getMillis(), config.getEndDate().getMillis());

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
				long[] dataStatus = getDataStatus(md);
				if ( (dataStatus[1] == -1) 
						|| (dataStatus[0] == TsrConstants.COLUMN_FAMILY_MAX_COLUMNS)
						|| ( (dataStatus[1] - dataStatus[0]) < maxDims.get(it)) )
					continue;
				
				int	nchunks = (int) Math.ceil((dataStatus[1] - dataStatus[0] + 1)*1.0/dataChunk);
				for (int i = 0; i < nchunks; i++) {
					long st = (i*dataChunk - maxDims.get(it));
					if (st < 0)
						st = 0;
					long et = ( ((i+1)*dataChunk -1) + maxDims.get(it));
					if (et > dataStatus[1])
						et = dataStatus[1];
					if (et > TsrConstants.COLUMN_FAMILY_MAX_COLUMNS)
						et = TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
					if (it.equals(IndexType.ptrn)) {
						for (int destinationLn : service.getReplicaSet(md.getCluster(), md.getId())) {
							service.addTask(new IndexingTask(md.getCluster(), destinationLn, md.getId(), st, et, index, CommandType.INSERT));
						}
					} else 
						service.addTask(new IndexingTask(md.getCluster(), md.getLn(), md.getId(), st, et, index, CommandType.INSERT));
				}	
			}
		}
	}

	protected List<String> getIndexDiffs(String oldIndexes, String newIndexes) {
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
