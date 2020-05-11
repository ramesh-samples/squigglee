package com.squigglee.coord.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.IndexingTask;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesConfig;

public class IConfigServiceImpl extends ICoordServiceImpl implements IConfigService {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.IConfigServiceImpl");
	
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork() {
		Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork = new HashMap<String,Map<String,Map<Integer, List<String>>>>();
		try {
			for (String cluster : zkov.getChildren(TsrConstants.ROOT_PATH, false)) {
				String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig";
				List<String> configEntries = zkov.getChildren(pathString, false);
				for (String lnentry : configEntries) {
					int ln = Integer.parseInt(lnentry);
					List<String> lnEntries = zkov.getChildren(pathString + "/" + ln, false);
					for (String entry : lnEntries) {
						String guid = new String(zkov.getData(pathString + "/" + ln + "/" + entry + "/guid", false, null));
						String datatype = new String(zkov.getData(pathString + "/" + ln + "/" + entry + "/datatype", false, null));
						updateNetwork(overlayNetwork, datatype, cluster, ln, guid);
					}
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting overlay network", e);
		} catch (InterruptedException e) {
			logger.error("Found error getting overlay network", e);
		}
		return overlayNetwork;
	}
	
	public MasterData getConfig(IndexingTask task) {
		MasterData md = null;
		if (task == null)
			return md;
		int ln = getLogicalNode(task.getId());
		try {
			String path = TsrConstants.ROOT_PATH + "/" + task.getCluster() + "/tsconfig/" + ln + "/" + getLocalIdString(task.getId());
			List<String> configEntries = zkov.getChildren(path, false);
			if (configEntries != null && configEntries.size() > 0) {
				boolean rollup = Boolean.parseBoolean(new String(zkov.getData(path + "/rollup", false, null)));
				String guid = new String (zkov.getData(path + "/guid", false, null) );
				assert ( task.getId() == Long.parseLong(new String (zkov.getData(path + "/id", false, null)) ));
				String datatype = new String (zkov.getData(path + "/datatype", false, null) );
				//String indexes = new String (zkov.getData(path + "/indexes", false, null) );
				byte[] indexesData = zkov.getData(path + "/indexes", false, null);
				String indexes = null;
				if (indexesData != null && indexesData.length > 0)
					indexes = new String (indexesData);
				
				Frequency frequency = Frequency.valueOf(new String (zkov.getData(path + "/freq", false, null)) );
				long startts = Long.parseLong(new String (zkov.getData(path + "/startts", false, null)));
				md = new MasterData(task.getCluster(), task.getId(), ln, guid, startts, frequency,	datatype, indexes, rollup);		
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + task.getCluster() + " and ln = " + ln + " and id = " + task.getId(), e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + task.getCluster() + " and ln = " + ln + " and id = " + task.getId(), e);
		}
		return md;
	}
	
	public Map<String,List<Long>> getConfig(String cluster, int ln, String guid) {
		Map<String,List<Long>> map = new HashMap<String,List<Long>>();
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			for (String entry : configEntries) {
				byte[] starttsdata = zkov.getData(pathString + entry + "/startts", false, null);
				long startts = Long.parseLong(new String(starttsdata));
				byte[] guiddata = zkov.getData(pathString + entry + "/guid", false, null);
				if ( !(new String(guiddata)).equalsIgnoreCase(guid) )
					continue;
				if (!map.containsKey(guid))
					map.put(guid, new ArrayList<Long>());
				if (!map.get(guid).contains(startts))
					map.get(guid).add(startts);
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		}
		return map;
	}
	
	public boolean isConfigured(String cluster, int ln, String guid) {
		if (guid == null)
			return false;
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			for (String entry : configEntries) {
				if (guid.equalsIgnoreCase(new String(zkov.getData(pathString + entry + "/guid", false, null))))
						return true;
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		}
		return false;
	}

	public Map<String,List<TimeSeriesConfig>> getConfig(String cluster, int ln) {
		Map<String,List<TimeSeriesConfig>> map = new HashMap<String,List<TimeSeriesConfig>>();
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			TimeSeriesConfig config = null;
			DateTime storagestart = null;
			DateTime storageend = null;
			for (String entry : configEntries) {
				boolean rollup = Boolean.parseBoolean(new String(zkov.getData(pathString + entry + "/rollup", false, null)));
				long startts = Long.parseLong(new String(zkov.getData(pathString + entry + "/startts", false, null)));
				String guid = new String(zkov.getData(pathString + entry + "/guid", false, null));
				//String id = new String(zkov.getData(pathString + entry + "/id", false, null));
				Frequency freq = Frequency.valueOf(new String(zkov.getData(pathString + entry + "/freq", false, null)));
				//String indexes = new String(zkov.getData(pathString + entry + "/indexes", false, null));
				byte[] indexesData = zkov.getData(pathString + entry + "/indexes", false, null);
				String indexes = null;
				if (indexesData != null && indexesData.length > 0)
					indexes = new String (indexesData);
				
				String datatype = new String(zkov.getData(pathString + entry + "/datatype", false, null));
				storagestart = new DateTime( startts, DateTimeZone.UTC);
				storageend = TimeSeriesShard.getMaxEndDate(freq, storagestart);
				config = new TimeSeriesConfig(cluster, guid, ln, freq, datatype, indexes, storagestart, storageend, rollup);
				
				if (!map.containsKey(guid))
					map.put(guid, new ArrayList<TimeSeriesConfig>());
				map.get(guid).add(config);
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for ln = " + ln, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for ln = " + ln, e);
		}
		for (String guid : map.keySet())
			map.put(guid, TimeSeriesConfig.collapseTimeIntervals(map.get(guid)));
		return map;
	}
	
	public Map<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>> getConfig() {
		Map<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>> map = new HashMap<String, Map<Integer, Map<String,List<TimeSeriesConfig>>>>();
		try {
			for (String cluster : zkov.getChildren(TsrConstants.ROOT_PATH, false)) {
				if (!map.containsKey(cluster))
					map.put(cluster, new HashMap<Integer, Map<String, List<TimeSeriesConfig>>>());
				map.get(cluster).putAll(getConfig(cluster));
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting global configuration", e);
		} catch (InterruptedException e) {
			logger.error("Found error getting global configuration", e);
		}
		return map;
	}
	
	public Map<Integer, Map<String,List<TimeSeriesConfig>>> getConfig(String cluster) {
		Map<Integer, Map<String,List<TimeSeriesConfig>>> map = new HashMap<Integer, Map<String,List<TimeSeriesConfig>>>();
		try {	
			for (String lnString : zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig", false)){
				int ln = Integer.parseInt(lnString);
				if (!map.containsKey(ln))
					map.put(ln, new HashMap<String, List<TimeSeriesConfig>>());
				Map<String, List<TimeSeriesConfig>> nodeConfig = getConfig(cluster, ln);
				for (String guid : nodeConfig.keySet())
					map.get(ln).put(guid, TimeSeriesConfig.collapseTimeIntervals(nodeConfig.get(guid)));
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting global configuration", e);
		} catch (InterruptedException e) {
			logger.error("Found error getting global configuration", e);
		}
		return map;
	}
	
	public List<Long> createConfig(TimeSeriesConfig config) {
		List<Long> requestList = TimeSeriesShard.getShardStartTimestamps(config.getFrequency(), config.getStartDate(), config.getEndDate());
		Map<String,List<Long>> currentMap = getConfig(config.getCluster(), config.getLogicalNode(), config.getGuid());
		List<Long> currentlyConfiguredList =  currentMap.get(config.getGuid());
		List<Long> addedEntries = new ArrayList<Long>();
		List<Long> todo = new ArrayList<Long>();
		if (currentlyConfiguredList != null) {
			for (Long c : currentlyConfiguredList) {
				if (!requestList.contains(c))
					todo.add(c);
				addedEntries.add(c);
			}
		}
		else {
			todo = requestList;
		}
		for (Long l : todo) {
			String entry = null;
			try {
				String pathString = TsrConstants.ROOT_PATH + "/" + config.getCluster() + "/tsconfig/" + config.getLogicalNode() + "/";
				//System.out.println(pathString);
				entry = zkov.create(pathString, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				String[] tokens = entry.split("/");
				long id = config.getStartToken() + Long.parseLong(tokens[tokens.length - 1]);
				
				this.ovNodeOperator.createNode(entry + "/id", ("" + id).getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/startts", ("" + l).getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/datatype", config.getDatatype().getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/freq", config.getFrequency().toString().getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/guid", config.getGuid().getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/min", ("" + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS).getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/max", ("-1").getBytes(), CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/indexes", 
					(config.getIndexes() == null || config.getIndexes().length() == 0)?(new byte[0]):config.getIndexes().getBytes(), 
					CreateMode.PERSISTENT);
				this.ovNodeOperator.createNode(entry + "/rollup", ("" + config.isRollup()).getBytes(), CreateMode.PERSISTENT);
				if (!addedEntries.contains(id))
					addedEntries.add(id);
			} catch (KeeperException e) {
				logger.error("Failed to create master data for configuration path = " + entry, e);
				System.out.println("Failed to create master data for configuration path = " + entry);
			} catch (InterruptedException e) {
				logger.error("Failed to create master data for configuration path = " + entry, e);
				System.out.println("Failed to create master data for configuration path = " + entry);
			}
		}
		return addedEntries;
	}
	
	public List<Long> deleteConfig(TimeSeriesConfig config) {
		List<Long> shardStartTimestamps = TimeSeriesShard.getShardStartTimestamps(config.getFrequency(), config.getStartDate(), config.getEndDate());
		String pathString = TsrConstants.ROOT_PATH + "/" + config.getCluster() + "/tsconfig/" + config.getLogicalNode() + "/";
		List<Long> deletedEntries = new ArrayList<Long>();
		try {
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + config.getCluster() + "/tsconfig" + "/" + config.getLogicalNode(), false);
			for (String entry : configEntries) {
				long startts = Long.parseLong(new String(zkov.getData(pathString + entry + "/startts", false, null)));
				String configuredGuid = new String(zkov.getData(pathString + entry + "/guid", false, null));
				long id = Long.parseLong(new String(zkov.getData(pathString + entry + "/id", false, null)));
				if (!shardStartTimestamps.contains(startts) || !config.getGuid().equalsIgnoreCase(configuredGuid))
					continue;
				this.ovNodeOperator.deleteNode(pathString + entry + "/startts");
				this.ovNodeOperator.deleteNode(pathString + entry + "/id");
				this.ovNodeOperator.deleteNode(pathString + entry + "/guid");
				this.ovNodeOperator.deleteNode(pathString + entry + "/ks");
				this.ovNodeOperator.deleteNode(pathString + entry + "/replication");
				this.ovNodeOperator.deleteNode(pathString + entry + "/strategy");
				this.ovNodeOperator.deleteNode(pathString + entry + "/datatype");
				this.ovNodeOperator.deleteNode(pathString + entry + "/indexes");
				this.ovNodeOperator.deleteNode(pathString + entry + "/rollup");
				this.ovNodeOperator.deleteNode(pathString + entry + "/freq");
				this.ovNodeOperator.deleteNode(pathString + entry + "/min");
				this.ovNodeOperator.deleteNode(pathString + entry + "/max");
				this.ovNodeOperator.deleteNode(pathString + entry);
				deletedEntries.add(id);
			}
		} catch (KeeperException e) {
			logger.error("Found error getting config for config = " + config, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for config = " + config, e);
		}
		return deletedEntries;
	}
	
	public MasterData getMasterData(String cluster, long id) {
		MasterData md = null;
		int ln = TimeSeriesConfig.getLogicalNode(id);
		int localid = (int) (id % TsrConstants.COLUMN_FAMILY_MAX_COLUMNS);
		String localidString = "" + localid;
		while (localidString.length() < 10)
			localidString = "0" + localidString;
		try {
			String path = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/" + localidString;
			String guid = new String (zkov.getData(path + "/guid", false, null) );
			assert ( id == Long.parseLong(new String (zkov.getData(path + "/id", false, null)) ));
			String datatype = new String (zkov.getData(path + "/datatype", false, null) );
			boolean rollup = Boolean.parseBoolean(new String(zkov.getData(path + "/rollup", false, null)));
			//String indexes = new String (zkov.getData(path + "/indexes", false, null) );
			byte[] indexesData = zkov.getData(path + "/indexes", false, null);
			String indexes = null;
			if (indexesData != null && indexesData.length > 0)
				indexes = new String (indexesData);
			
			Frequency frequency = Frequency.valueOf(new String (zkov.getData(path + "/freq", false, null)) );
			long startts = Long.parseLong(new String (zkov.getData(path + "/startts", false, null)));
			md = new MasterData(cluster, id, ln, guid, startts, frequency, datatype, indexes, rollup);				
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and id = " + id, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and id = " + id, e);
		}
		return md;
	}
	
	public List<Long> getMasterDataIds(String cluster, int ln, List<String> guids) {
		List<Long> idList = new ArrayList<Long>();
		if (guids == null || guids.isEmpty())
			return idList;
		try {
			String path = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln;
			List<String> configEntries = zkov.getChildren(path, false);
			if (configEntries != null && configEntries.size() > 0) {
				for (String configEntry : configEntries) {
					String configguid = new String (zkov.getData(path + "/" + configEntry + "/guid", false, null) );
					if (guids.contains(configguid))
						idList.add(Long.parseLong(new String (zkov.getData(path + "/" + configEntry + "/id", false, null))));
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guids = " + guids, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guids = " + guids, e);
		}
		return idList;
	}
	
	public List<MasterData> getMasterData(String cluster, int ln, String guid) {
		return getConfiguredMasterData(cluster, ln, guid, -1, -1);
	}
	
	/**
	 * 
	 * @param cluster -- cluster identifier, cannot be null  
	 * @param ln -- logical node identifier, must be >= 0 
	 * @param guid -- unique parameter identifier, null to get all 
	 * @param startts -- represents the start of any time interval of interest, not necessarily a shard start time, -1 to skip this check 
	 * @param endts -- represents the end of any time interval of interest, -1 to skip this check 
	 * @return master data list -- sorted in time ascending shard order 
	 */
	public List<MasterData> getConfiguredMasterData(String cluster, int ln, String guid, long startts, long endts) {
		List<MasterData> list = new ArrayList<MasterData>();
		try {
			SortedMap<Long,String> starttsMap = new TreeMap<Long,String>();
			String path = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln;
			List<String> configEntries = zkov.getChildren(path, false);
			long maxstartts = -1L;
			if (configEntries != null && configEntries.size() > 0) {
				for (String configEntry : configEntries) {
					String configguid = new String (zkov.getData(path + "/" + configEntry + "/guid", false, null) );
					if (!configguid.equalsIgnoreCase(guid))
						continue;
					long cfgstartts = Long.parseLong(new String (zkov.getData(path + "/" + configEntry + "/startts", false, null)));
					//skip the higher shards, that start later than the interval end time 
					if (endts >= 0 && cfgstartts > endts)
						continue;
					//put the rest in the map initially but track the shard that is <= but closest to the interval start time 
					if (startts >= 0 && maxstartts < cfgstartts && cfgstartts <= startts)
						maxstartts = cfgstartts;
					starttsMap.put(cfgstartts, configEntry);
				}
				//remove the extraneous lower shards 
				if (startts >= 0)
					starttsMap = starttsMap.tailMap(maxstartts);
				for (long ts : starttsMap.keySet()) {
					String configEntry = starttsMap.get(ts);
					long id = Long.parseLong(new String (zkov.getData(path + "/" + configEntry + "/id", false, null)) );
					String datatype = new String (zkov.getData(path + "/" + configEntry + "/datatype", false, null) );
					boolean rollup = Boolean.parseBoolean(new String(zkov.getData(path + "/" + configEntry + "/rollup", false, null)));
					byte[] indexesData = zkov.getData(path + "/" + configEntry + "/indexes", false, null);
					String indexes = null;
					if (indexesData != null && indexesData.length > 0)
						indexes = new String (indexesData);
					Frequency frequency = Frequency.valueOf(new String (zkov.getData(path + "/" + configEntry + "/freq", false, null)) );
					list.add(new MasterData(cluster, id, ln, guid, ts, frequency, datatype, indexes, rollup));
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " and ln = " + ln + " and guid = " + guid, e);
		}
		return list;
	}
	
	private void updateNetwork(Map<String,Map<String, Map<Integer, List<String>>>> overlayNetwork, String dataType, String cluster, int ln, String guid) {
		if (!overlayNetwork.containsKey(dataType))
			overlayNetwork.put(dataType, new HashMap<String, Map<Integer,List<String>>>());
		if (!overlayNetwork.get(dataType).containsKey(cluster))
			overlayNetwork.get(dataType).put(cluster, new HashMap<Integer, List<String>>());
		if (!overlayNetwork.get(dataType).get(cluster).containsKey(ln))
			overlayNetwork.get(dataType).get(cluster).put(ln, new ArrayList<String>());
		if (!overlayNetwork.get(dataType).get(cluster).get(ln).contains(guid))
			overlayNetwork.get(dataType).get(cluster).get(ln).add(guid);
	}

	public Map<Long,Map<IndexType,List<String>>> getIndexList(String cluster, int ln) {
		Map<Long,Map<IndexType,List<String>>> map = new HashMap<Long,Map<IndexType,List<String>>>();
		//example "ptrn_16_1000_100_8_1000;skchCM_1_3599999_1024_50_1000"
		//List<String> indexes = new ArrayList<String>();
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			for (String entry : configEntries) {
				long id = Long.parseLong(new String(zkov.getData(pathString + entry + "/id", false, null)) );
				String index = new String(zkov.getData(pathString + entry + "/indexes", false, null));
				if (index == null || index.length() == 0)
					continue;
				String[] indexes = index.split(";");
				for (IndexType supportedIndexType : IndexType.values()) {
					for (String idx : indexes) {
						if (idx.toLowerCase().startsWith(supportedIndexType.name().toLowerCase())) {
							if (!map.containsKey(id))
								map.put(id, new HashMap<IndexType,List<String>>());
							if (!map.get(id).containsKey(supportedIndexType))
								map.get(id).put(supportedIndexType, new ArrayList<String>());
							if (!map.get(id).get(supportedIndexType).contains(idx))
								map.get(id).get(supportedIndexType).add(idx);
						}
					}
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for ln = " + ln, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for ln = " + ln, e);
		}
		return map;
	}
	
	public Map<String,Map<IndexType,List<String>>> getGuidIndexList(String cluster, int ln) {
		Map<String,Map<IndexType,List<String>>> map = new HashMap<String,Map<IndexType,List<String>>>();
		//example "ptrn_16_1000_100_8_1000;skchCM_1_3599999_1024_50_1000"
		//List<String> indexes = new ArrayList<String>();
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			for (String entry : configEntries) {
				String guid = new String(zkov.getData(pathString + entry + "/guid", false, null));
				String index = new String(zkov.getData(pathString + entry + "/indexes", false, null));
				if (index == null || index.length() == 0)
					continue;
				String[] indexes = index.split(";");
				for (IndexType supportedIndexType : IndexType.values()) {
					for (String idx : indexes) {
						if (idx.toLowerCase().startsWith(supportedIndexType.name().toLowerCase())) {
							if (!map.containsKey(guid))
								map.put(guid, new HashMap<IndexType,List<String>>());
							if (!map.get(guid).containsKey(supportedIndexType))
								map.get(guid).put(supportedIndexType, new ArrayList<String>());
							if (!map.get(guid).get(supportedIndexType).contains(idx))
								map.get(guid).get(supportedIndexType).add(idx);
						}
					}
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for ln = " + ln, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for ln = " + ln, e);
		}
		return map;
	}
	
	public List<String> getGuidIndexList(String cluster, int ln, IndexType it) {
		List<String> list = new ArrayList<String>();
		//example "ptrn_16_1000_100_8_1000;skchCM_1_3599999_1024_50_1000"
		//List<String> indexes = new ArrayList<String>();
		try {
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln + "/";
			List<String> configEntries = zkov.getChildren(TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + ln, false);
			for (String entry : configEntries) {
				String guid = new String(zkov.getData(pathString + entry + "/guid", false, null));
				String index = new String(zkov.getData(pathString + entry + "/indexes", false, null));
				if (index == null || index.length() == 0)
					continue;
				for (String idx : index.split(";")) {
					if (idx.toLowerCase().startsWith(it.name().toLowerCase())) {
						if (!list.contains(guid))
							list.add(guid);
					}
				}
			}
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.error("Found error getting config for ln = " + ln, e);
		} catch (InterruptedException e) {
			logger.error("Found error getting config for ln = " + ln, e);
		}
		return list;
	}

	public int createMasterData(List<MasterData> list) {
		int count = 0;
		for (MasterData md : list) {
			String pathString = TsrConstants.ROOT_PATH + "/" + md.getCluster() + "/tsconfig/" + md.getLn();
			String entry = getLocalIdString(md.getId());
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/id", ("" + md.getId()).getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/guid", md.getGuid().getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/startts", ("" + md.getStartts()).getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/freq", md.getFreq().toString().getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/datatype", md.getDatatype().getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/indexes", (md.getIndexes()==null?"":md.getIndexes()).getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/rollup", ("" + md.isRollup()).getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/min", ("" + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS).getBytes(), CreateMode.PERSISTENT);
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/max", ("-1").getBytes(), CreateMode.PERSISTENT);
			count++;
		}
		return count;
	}
	
	public int updateMasterData(List<MasterData> list) {
		int count = 0;
		for (MasterData md : list) {
			String pathString = TsrConstants.ROOT_PATH + "/" + md.getCluster() + "/tsconfig/" + md.getLn();
			String entry = getLocalIdString(md.getId());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/id", ("" + md.getId()).getBytes());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/guid", md.getGuid().getBytes());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/startts", ("" + md.getStartts()).getBytes());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/freq", md.getFreq().toString().getBytes());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/datatype", md.getDatatype().getBytes());
			this.ovNodeOperator.setData(pathString + "/" + entry + "/indexes", (md.getIndexes()==null?"":md.getIndexes()).getBytes());
			this.ovNodeOperator.createNode(pathString + "/" + entry + "/rollup", ("" + md.isRollup()).getBytes(), CreateMode.PERSISTENT);
			count++;
		}
		return count;
	}
	
	public void updateDataMinMax(MasterData md, long min, long max) {
		String pathString = TsrConstants.ROOT_PATH + "/" + md.getCluster() + "/tsconfig/" + md.getLn();
		String entry = getLocalIdString(md.getId());
		try {
			long currentMin = Long.parseLong(new String(zkov.getData(pathString + "/" + entry + "/min", false, null)));
			long currentMax = Long.parseLong(new String(zkov.getData(pathString + "/" + entry + "/max", false, null)));
			if (min < currentMin)
				this.ovNodeOperator.setData(pathString + "/" + entry + "/min", ("" + min).getBytes());
			if (max > currentMax)
				this.ovNodeOperator.setData(pathString + "/" + entry + "/max", ("" + max).getBytes());
		} catch (NumberFormatException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		} catch (KeeperException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		} catch (InterruptedException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		}
	}
	
	public long[] getDataMinMax(MasterData md) {
		String pathString = TsrConstants.ROOT_PATH + "/" + md.getCluster() + "/tsconfig/" + md.getLn();
		String entry = getLocalIdString(md.getId());
		long currentMin = TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
		long currentMax = -1;
		try {
			currentMin = Long.parseLong(new String(zkov.getData(pathString + "/" + entry + "/min", false, null)));
			currentMax = Long.parseLong(new String(zkov.getData(pathString + "/" + entry + "/max", false, null)));
		} catch (NumberFormatException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		} catch (KeeperException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		} catch (InterruptedException e) {
			logger.error("Found error updating data min + max for cluster = " + md.getCluster() + " and masterdata = " + md, e);
		}
		return new long[]{currentMin, currentMax};
	}
	
	public boolean isConfiguredWithIndex(String cluster, long id, String index) {
		if (index == null)
			return false;
		try {
			String localid = getLocalIdString(id);
			String pathString = TsrConstants.ROOT_PATH + "/" + cluster + "/tsconfig/" + getLogicalNode(id);
			long configuredId = Long.parseLong(new String(zkov.getData(pathString + "/" + localid + "/id", false, null)) );
			if (configuredId != id) {
				logger.error("Configuration error: configured id = " + configuredId + " does not match requested id = " + id + " for cluster = " + cluster);
				return false;
			}
			String currentIndex = new String(zkov.getData(pathString + "/" + localid + "/indexes", false, null));
			if (currentIndex == null || !(currentIndex + "_" + id).contains(index))
				return false;
		} catch (KeeperException e) {
			if (!e.code().equals(KeeperException.Code.NONODE))
				logger.debug("No config for id = " + id + " and index = " + index, e);
			else
				logger.error("Found error getting config for cluster = " + cluster + " id = " + id + " and index = " + index);
			return false;
				
		} catch (InterruptedException e) {
			logger.error("Found error getting config for cluster = " + cluster + " id = " + id + " and index = " + index);
			return false;
		}
		return true;
	}

}
