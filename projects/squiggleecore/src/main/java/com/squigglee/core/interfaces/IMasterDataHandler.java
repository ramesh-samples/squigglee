// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;

public interface IMasterDataHandler extends ISchemaHandler {
	
	public List<TimeSeriesConfig> getMasterData(String cluster, int ln, String guid) throws TimeSeriesException;
	public MasterData getMasterData(String cluster, long id) throws TimeSeriesException;
	public List<MasterData> getMasterData(String cluster, List<Long> ids) throws TimeSeriesException;
	public MasterData getMasterData(String cluster, int ln, String guid, long startts) throws TimeSeriesException;	// gets the closest record at or below the timestamp
	public List<Long> getMasterDataIds(String cluster, int ln, List<String> guids) throws TimeSeriesException;
	public List<MasterData> getMasterData(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException;	//unions the range and the closest record at or below the start
	public List<TimeSeriesConfig> getMasterDataConfig(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException;
	public List<Long> updateMasterData(TimeSeriesConfig config) throws TimeSeriesException;
	public List<Long> deleteMasterData(TimeSeriesConfig config) throws TimeSeriesException;
	public List<Long> createMasterData(TimeSeriesConfig config) throws TimeSeriesException;
	public List<MasterData> getMasterDataForBlocks(String cluster, int ln, String guid, long startts, long endts) throws TimeSeriesException;	////unions the range and the closest record at or below the start
	public List<TimeSeriesConfig> getNodeMasterData(String cluster, int ln) throws TimeSeriesException;
	public List<TimeSeriesConfig> getGlobalConfig() throws TimeSeriesException;
	public List<TimeSeriesConfig> getClusterConfig(String cluster) throws TimeSeriesException;
	public long[] getDataStatus(MasterData md);
	public void updateDataStatus(MasterData md, long min, long max) throws TimeSeriesException;
	public NodeStatus getAlternateLocation(String cluster, int dataln) throws TimeSeriesException;
	public NodeStatus getLocation(String cluster, int nodeln) throws TimeSeriesException;
	public List<Integer> getReplicaSet(String cluster, int dataln) throws TimeSeriesException;
	public Map<String,Map<String, Map<Integer, List<String>>>> getOverlayNetwork() throws TimeSeriesException;
	public void setupCluster(String cluster) throws TimeSeriesException;
	public void updateNode(String cluster, int ln, String addr, String dataCenter, String instanceId, String name, boolean isBoot, boolean isSeed, 
			int replicaOf, int storage, String stype) throws TimeSeriesException;
	
	public int updateIndex(String cluster, int ln, String guid, String indexes, boolean drop) throws TimeSeriesException;
}
