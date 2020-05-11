// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;

public class MasterDataHandlerMixin {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.storage.MasterDataHandlerMixin");
	protected Random rand = null;
	
	public MasterDataHandlerMixin() {
		this.rand = new Random();
	}
	
	public MasterData getMasterData(String cluster, int ln, String guid, long startts) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(cluster, ln, guid, startts, -1);
		if (list != null && list.size() > 0)
			return list.get(0);
		else
			return null;
	}

	public List<TimeSeriesConfig> getMasterData(String cluster, int ln, String guid)
			throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(cluster, ln, guid, -1, -1);
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		TimeSeriesConfig config = null;
		DateTime storagestart = null;
		DateTime storageend = null;
		
		for (MasterData md : list) {		
			storagestart = new DateTime( md.getStartts(), DateTimeZone.UTC);
			storageend = TimeSeriesShard.getMaxEndDate(md.getFreq(), storagestart);
			config = new TimeSeriesConfig(cluster, guid, md.getLn(), md.getFreq(), md.getDatatype(),
				md.getIndexes(), storagestart, storageend);
			configs.add(config);
		}
		if (configs == null || configs.isEmpty()) {
			logger.debug("No master data configured for cluster = " + cluster + " and id = " + guid + " and ln = " + ln);
			System.out.println("No master data configured for cluster = " + cluster + " and id = " + guid + " and ln = " + ln);
		}
		return TimeSeriesConfig.collapseTimeIntervals(configs);
	}
	
	public List<TimeSeriesConfig> getMasterDataConfig(String cluster, int ln, String guid, long startts, long endts)
			throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(cluster, ln, guid, -1, -1);
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		TimeSeriesConfig config = null;
		
		for (MasterData md : list) {		
			if (md.getStartts() >= startts && md.getStartts() <= endts) {
				DateTime storagestart = new DateTime( md.getStartts(), DateTimeZone.UTC);
				config = new TimeSeriesConfig(cluster, guid, md.getLn(), md.getFreq(), md.getDatatype(),
						md.getIndexes(), storagestart, TimeSeriesShard.getMaxEndDate(md.getFreq(), storagestart));
				configs.add(config);
			}
		}
		if (configs == null || configs.isEmpty()) {
			logger.debug("No master data configured for cluster = " + cluster + " and id = " + guid + " and ln = " + ln + " and ts range [" + startts + "," + endts + "]");
			System.out.println("No master data configured for cluster = " + cluster + " and id = " + guid + " and ln = " + ln + " and ts range [" + startts + "," + endts + "]");
		}
		return TimeSeriesConfig.collapseTimeIntervals(configs);
	}
	
	public void setupCluster(String cluster) throws TimeSeriesException {
		IDataService dataService = ServiceFactory.getDataService();
		dataService.setupCluster(cluster);
	}
	
	public void updateNode(String cluster, int ln, String addr, String dataCenter, String instanceId, String name, boolean isBoot, boolean isSeed, 
			int replicaOf, int storage, String stype) throws TimeSeriesException {
		IDataService dataService = ServiceFactory.getDataService();
		dataService.setNode(cluster, ln, addr, dataCenter, instanceId, isBoot, isSeed, name, replicaOf, storage, stype);
		logger.debug("Updated node status for node = " + ln);
	}
	
	public NodeStatus getAlternateLocation(String cluster, int dataln, String localDataCenter)  throws TimeSeriesException {
		
		IStatusService statusService = ServiceFactory.getStatusService();
		List<NodeStatus> replicaStatus = statusService.getReplicaStatus(cluster, dataln);
		logger.debug("Found " + replicaStatus.size() + " candidates for an alternate location for data ln = " + dataln 
				+ " and cluster = " + cluster);
		List<NodeStatus> local = new ArrayList<NodeStatus>();
		List<NodeStatus> remote = new ArrayList<NodeStatus>();
		for (NodeStatus ns : replicaStatus) {
			//skip unavailable nodes 
			if (!ns.isNodeUp() || !ns.isOverlayUp())
				continue;
			
			if (ns.getDataCenter().equalsIgnoreCase(localDataCenter))
				local.add(ns);
			else
				remote.add(ns);
		}
		
		//load balance randomly in the case of multiple options with preference to the local data center
		if (local.size() > 0)
			return local.get(rand.nextInt(local.size()));
		else if (remote.size() > 0)
			return remote.get(rand.nextInt(remote.size()));
		
		logger.error("Failed to find an alternate location for data ln = " + dataln);
		return null;
	}
	
	public NodeStatus getLocation(String cluster, int nodeln)  throws TimeSeriesException {
		return ServiceFactory.getStatusService().getNodeStatus(cluster, nodeln);
	}
	
	public void updateDataStatus(MasterData md, long min, long max) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		configurationService.updateDataMinMax(md, min, max);
	}
	
	public long[] getDataStatus(MasterData md) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		return configurationService.getDataMinMax(md);
	}

	public List<TimeSeriesConfig> getNodeMasterData(String cluster, int ln)
			throws TimeSeriesException {
		
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
		Map<String,List<TimeSeriesConfig>> vals = configurationService.getConfig(cluster, ln);
		for (String guid : vals.keySet())
			configs.addAll(vals.get(guid));
		//configurationService.close();
		if (configs.isEmpty()) {
			logger.debug("No master data configured for cluster = " + cluster + " and ln = " + ln);
			System.out.println("No master data configured for cluster = " + cluster + " and ln = " + ln);
		}
		return TimeSeriesConfig.collapseTimeIntervals(configs);
	}
	
	public Map<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>> getConfig() throws TimeSeriesException {
		return ServiceFactory.getConfigurationService().getConfig();
	}
	
	public Map<Integer, Map<String,List<TimeSeriesConfig>>> getConfig(String cluster) throws TimeSeriesException {
		return ServiceFactory.getConfigurationService().getConfig(cluster);
	}
	
	public MasterData getMasterData(String cluster, long id) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		return configurationService.getMasterData(cluster, id);
	}
	
	public List<MasterData> getMasterData(String cluster, List<Long> ids) throws TimeSeriesException {
		List<MasterData> mdList = new ArrayList<MasterData>();
		for (long id : ids)
			mdList.add(getMasterData(cluster, id));
		return mdList;
	}

	public List<MasterData> getMasterData(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		List<MasterData> list = configurationService.getConfiguredMasterData(cluster, ln, guid, startts, endts);
		//configurationService.close();
		return list;
	}

	public List<Integer> getReplicaSet(String cluster, int dataln) throws TimeSeriesException {
		return ServiceFactory.getCoordinationService().getReplicaSet(cluster, dataln);
	}
	
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork() throws TimeSeriesException {
		return ServiceFactory.getConfigurationService().getOverlayNetwork();
	}
	
	public int deleteIndexRecords(List<MasterData> list, String index, IIndexService service) throws TimeSeriesException {
		for (MasterData md : list) {
			service.deleteSerializedIndex(md.getCluster(), md.getLn(), md.getId(), index + "_" + md.getId());
		}
		return list.size();
	}
	
	public List<Long> getMasterDataIds(String cluster, int ln, List<String> guids) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		return configurationService.getMasterDataIds(cluster, ln, guids);
	}
}
