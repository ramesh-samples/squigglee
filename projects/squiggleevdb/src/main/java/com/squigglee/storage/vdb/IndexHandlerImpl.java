// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.vdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.squigglee.coord.interfaces.IConfig;
import com.squigglee.coord.interfaces.ICoord;
import com.squigglee.coord.interfaces.IIndex;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;

public class IndexHandlerImpl extends MasterDataHandlerImpl implements IIndexHandler {
	
	private static Logger logger = Logger.getLogger("com.squigglee.storage.vdb.IndexHandlerImpl");
	public IndexHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}

	@Override
	public byte[] loadSerializedIndex(long id, String idxTableName) throws TimeSeriesException {
		byte[] serialized = null;
		try {
			IIndex indexService = serviceFactory.getIndexService();
			serialized = indexService.loadSerializedIndex(this.clusterName, id, idxTableName);
			indexService.close();
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return serialized;		
	}

	@Override
	public void saveSerializedIndex(long id, String idxTableName, byte[] srlIndex, boolean create) {
		if (srlIndex == null)
			return;
		
		if (!getConfigurationStatus(id,idxTableName)) {
			System.out.println("Index not (no longer) longer configured, skipping update for id = " + id + " index = " + idxTableName);
			logger.debug("Index not (no longer) configured, skipping update for id = " + id + " index = " + idxTableName);
			return;
		}

		try {
			IIndex indexService = serviceFactory.getIndexService();
			indexService.saveSerializedIndex(this.clusterName, id, idxTableName, srlIndex, create);
			indexService.close();
			
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
	}

	@Override
	public Map<Long,Map<IndexType,List<String>>> getIndexList(int ln) throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		Map<Long,Map<IndexType,List<String>>> map = configurationService.getIndexList(this.clusterName, ln);
		configurationService.close();
		return map;
	}
	
	@Override
	public List<String> parseIndexString(IndexType indexType, String indexString) {
		if (indexString == null || indexString.length() == 0)
			return null;
		List<String> indexList = new ArrayList<String>(); 
		String[] indexes = indexString.split(";");
		for (String index : indexes) {
			if (index.toLowerCase().startsWith(indexType.name().toLowerCase()))
				indexList.add(index);		
		}
		return indexList;
	}

	@Override
	public int updateIndex(int ln, String guid, String indexes, boolean drop) throws TimeSeriesException {
		IConfig configurationService = serviceFactory.getConfigurationService();
		IIndex indexService = serviceFactory.getIndexService();
		
		int result = 0;
		List<MasterData> list = configurationService.getConfiguredMasterData(this.clusterName, ln, guid, -1, -1);
		if (list == null || list.size() == 0)
			return 0;
	
		String currentIndex = list.get(0).getIndexes();
		for (String index : indexes.split(";")) {
			if (drop) {
				if (currentIndex.contains(index))
					currentIndex = currentIndex.replace(index, "");
				if (currentIndex.contains(";;"))
					currentIndex = currentIndex.replaceAll(";;", ";");
				if (currentIndex.endsWith(";"))
					currentIndex = currentIndex.substring(0,currentIndex.length()-1);
				if (currentIndex.startsWith(";"))
					currentIndex = currentIndex.substring(1,currentIndex.length());
				deleteIndexRecords(list, index, indexService);
				if (index.toLowerCase().contains("ptrn")) {
					deletePatternIndexTables(list,index);
				}
			}
			else {
				if (currentIndex == null || currentIndex.length() == 0)
					currentIndex = index;
				else {
					if (!currentIndex.contains(index))
						currentIndex += ";" + index;
				}
			}
		}
		List<MasterData> updatedList = new ArrayList<MasterData>();
		for (MasterData md : list) {
			updatedList.add(new MasterData(md.getId(), md.getLn(), md.getGuid(), md.getStartts(), md.getFreq(), 
			md.getDatatype(), currentIndex));
		}
		result = configurationService.updateMasterData(this.clusterName, updatedList);
		configurationService.close();
		indexService.close();
		result = list.size();
		return result;
	}
	
	@Override
	public boolean getConfigurationStatus(long id, String index) {
		try {
			IConfig configurationService = serviceFactory.getConfigurationService();
			boolean status = configurationService.isConfiguredWithIndex(this.clusterName, id, index);
			configurationService.close();
			return status;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return false;
	}
	
	@Override
	public List<Integer> getReplicaSet(long id) {
		try {
			ICoord coordService = serviceFactory.getCoordinationService();
			List<Integer> result = coordService.getReplicaSet(this.clusterName, id);
			coordService.close();
			return result;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return null;
	}
	
	private int deleteIndexRecords(List<MasterData> list, String index, IIndex service) throws TimeSeriesException {
		
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return 0;
    	
		for (MasterData md : list) {
			service.deleteSerializedIndex(this.clusterName, md.getId(), index + "_" + md.getId());
		}
		return list.size();
	}
	
}
