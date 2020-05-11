package com.squigglee.api.rest.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.MatrixParam;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.api.rest.IConfigRESTService;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class ConfigRESTService extends RestBase implements IConfigRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.ConfigRESTService");
	

	@Override
	public String getMasterDataStatusJSON(MasterData md) {
		String status = null;
		try {
			logger.debug("Received request for status of master data " + md);
			System.out.println("Received request for status of master data " + md);
			long[] statusArr =  HandlerFactory.getMasterDataHandler().getDataStatus(md);
			status = statusArr[0] + ";" + statusArr[1];
		} catch (Exception e) {
			logger.error(("Found error fetching status of master data " + md), e);
			status = "-1;-1";
		}
		return status;
	}

	
	@Override
	public List<MasterData> getMasterDataJSON(@MatrixParam("cluster") String cluster, @MatrixParam("ln") int ln, @MatrixParam("id") String id, 
			@MatrixParam("startts") long start, @MatrixParam("endts") long end) {
		try {
			logger.debug("Received request for master data for time series config for cluster " + cluster + " node " + ln
					+ " id " + id + " start " + start + " end " + end);
			System.out.println("Received request for master data for time series config for cluster " + cluster + " node " + ln
					+ " id " + id + " start " + start + " end " + end);
			return HandlerFactory.getMasterDataHandler().getMasterData(cluster, ln, id, start, end);
		} catch (Exception e) {
			logger.error("Found error fetching fetching master data for time series config for cluster " + cluster + " node " + ln
					+ " id " + id + " start " + start + " end " + end, e);
			List<MasterData> list = new ArrayList<MasterData>();
			return list;
		}
	}
	
	
	@Override
	public List<TimeSeriesConfig> getGlobalConfigJSON() {
		try {
			logger.debug("Received request for global configuration data");
			System.out.println("Received request for global configuration data");
			return HandlerFactory.getMasterDataHandler().getGlobalConfig();
		} catch (Exception e) {
			logger.error("Found error fetching global configuration entries", e);
			List<TimeSeriesConfig> list = new ArrayList<TimeSeriesConfig>();
			return list;
		}
	}
	
	@Override
	public List<TimeSeriesConfig> getConfigJSON(String cluster, int ln, String id, long startts, long endts) {
		try {
			logger.debug("Received request for configuration data for cluster " + cluster + " and ln " + ln + " for id = " + id + " and time range [" + startts + "," + endts + "]");
			System.out.println("Received request for configuration data for cluster " + cluster + " and ln " + ln + " for id = " + id + " and time range [" + startts + "," + endts + "]");
			List<TimeSeriesConfig> list = HandlerFactory.getMasterDataHandler().getMasterDataConfig(cluster, ln, id, startts, endts);
			System.out.println(list);
			return list;
			//return mdHandler.getMasterDataConfig(cluster, ln, id, startts, endts);
		} catch (Exception e) {
			logger.error("Found error fetching configuration entries for ln = " 
				+ ln + " and id = " + id + " start = " + startts + " end = " + endts, e);
			List<TimeSeriesConfig> list = new ArrayList<TimeSeriesConfig>();
			TimeSeriesConfig tsc = new TimeSeriesConfig(cluster, id, ln, null);
			tsc.setStartDate(new DateTime(startts, DateTimeZone.UTC));
			tsc.setEndDate(new DateTime(endts, DateTimeZone.UTC));
			tsc.setErrorMessage(e.getMessage());
			list.add(tsc);
			return list;
		}
	}

	@Override
	public List<TimeSeriesConfig> getConfigJSON(String cluster, int ln, String id) {
		try {
			logger.debug("Received request for configuration data for cluster " + cluster + " and ln " + ln + " for id = " + id);
			System.out.println("Received request for configuration data for cluster " + cluster + " and ln " + ln + " for id = " + id);
			return HandlerFactory.getMasterDataHandler().getMasterData(cluster, ln, id);
		} catch (Exception e) {
			logger.error("Found error fetching configuration entries for ln = " + ln + " and id = " + id, e);
			List<TimeSeriesConfig> list = new ArrayList<TimeSeriesConfig>();
			TimeSeriesConfig tsc = new TimeSeriesConfig(cluster, id, ln, null);
			tsc.setErrorMessage(e.getMessage());
			list.add(tsc);
			return list;
		}
	}
	
	@Override
	public List<TimeSeriesConfig> getConfigJSON(String cluster, int ln) {
		try {
			logger.debug("Received request for configuration data for cluster " + cluster + " and ln " + ln);
			System.out.println("Received request for configuration data for cluster " + cluster + " and ln " + ln);
			return HandlerFactory.getMasterDataHandler().getNodeMasterData(cluster, ln);
		} catch (Exception e) {
			logger.error("Found error fetching configuration entries for ln = " + ln, e);
			List<TimeSeriesConfig> list = new ArrayList<TimeSeriesConfig>();
			TimeSeriesConfig tsc = new TimeSeriesConfig(cluster, null, ln, null);			
			tsc.setErrorMessage(e.getMessage());
			list.add(tsc);
			return list;
		}
	}

	@Override
	public TimeSeriesConfig createConfigJSON(TimeSeriesConfig config) {
		logger.debug("Received request to create configuration " + config);
		System.out.println("Received request to create configuration " + config);	
		return configProxy.createConfig(config);
	}

	@Override
	public TimeSeriesConfig updateConfigJSON(TimeSeriesConfig config) {
		logger.debug("Received request to update configuration " + config);
		System.out.println("Received request to update configuration " + config);			
		return configProxy.createConfig(config);
	}

	@Override
	public TimeSeriesConfig deleteConfigJSON(TimeSeriesConfig config) {
		logger.debug("Received request to delete configuration " + config);
		System.out.println("Received request to delete configuration " + config);					
		return configProxy.deleteConfig(config);
	}

	@Override
	public List<Integer> getReplicaSetJSON(String cluster, int ln) {
		try {
			logger.debug("Received request for replica set for node " + ln + " in cluster " + cluster);
			System.out.println("Received request for replica set for node " + ln + " in cluster " + cluster);
			return HandlerFactory.getIndexHandler().getReplicaSet(cluster, ln);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching replica set for cluster = " + cluster + " and ln = " + ln, e);
		}
		List<Integer> list = new ArrayList<Integer>();
		list.add(ln);
		return list;
	}

	@Override
	public void addIndexJSON(TimeSeriesConfig config) {
		logger.debug("Received request to add indexes for config " + config);
		System.out.println("Received request to add indexes for config " + config);
		configProxy.addIndex(config);
	}
	
	@Override
	public void dropIndexJSON(TimeSeriesConfig config) {
		logger.debug("Received request to drop indexes for config " + config);
		System.out.println("Received request to drop indexes for config " + config);
		configProxy.dropIndex(config);
	}

	@Override
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork() {
		Map<String,Map<String, Map<Integer, List<String>>>> map = new HashMap<String,Map<String, Map<Integer, List<String>>>>();
		try {
			logger.debug("Received request for overlay");
			System.out.println("Received request for overlay");
			map = HandlerFactory.getMasterDataHandler().getOverlayNetwork();
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching overlay", e);
		}
		return map;
	}
	
	@Override
	public Boolean setupCluster(String cluster) {
		try {
			logger.debug("Received request for initial setup of cluster " + cluster);
			System.out.println("Received request for initial setup of cluster " + cluster);
			HandlerFactory.getMasterDataHandler().setupCluster(cluster);
			return true;
		} catch (TimeSeriesException e) {
			logger.error("", e);
			return false;
		}
	}
	
	@Override
	public NodeStatus updateNode(String cluster, int ln, String addr,
			String dataCenter, String instanceId, String name, boolean isBoot,
			boolean isSeed, int replicaOf, int storage, String stype) {
		NodeStatus ns = new NodeStatus();
		ns.setCluster(cluster);
		ns.setLogicalNumber(ln);
		ns.setAddress(addr);
		ns.setDataCenter(dataCenter);
		ns.setInstanceId(instanceId);
		ns.setName(name);
		ns.setBootstrapNode(isBoot);
		ns.setSeedNode(isSeed);
		ns.setReplicaOf(replicaOf);
		ns.setStorage(storage);
		ns.setStype(stype);
		try {
			HandlerFactory.getMasterDataHandler().updateNode(cluster, ln, addr, dataCenter, instanceId, name, isBoot, isSeed, replicaOf, storage, stype);
			logger.debug("Updated data for node " + ln + " in cluster" + cluster + " with addr = " + addr + " instanceId = " + instanceId + 
					" name = " + name + " isBoot = " + isBoot + " isSeed = " + isSeed + " replicaOf = " + replicaOf + " storage = " + storage 
					+ " server type = " + stype);
			System.out.println("Updated data for node " + ln + " in cluster" + cluster + " with addr = " + addr + " instanceId = " + instanceId + 
					" name = " + name + " isBoot = " + isBoot + " isSeed = " + isSeed + " replicaOf = " + replicaOf + " storage = " + storage 
					+ " server type = " + stype);
		} catch (TimeSeriesException e) {
			logger.error("Found error updating node " + ns, e);
			ns.setErrorMessage(e.getMessage());
		}
		return ns;
	}

}
