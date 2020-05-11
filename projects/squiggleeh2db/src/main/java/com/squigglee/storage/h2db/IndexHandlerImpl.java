// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IConfig;
import com.squigglee.coord.interfaces.IIndex;
import com.squigglee.coord.storage.IndexHandlerMixin;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;

public class IndexHandlerImpl extends DataHandlerImpl implements IIndexHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.IndexHandlerImpl");
	private IndexHandlerMixin ihMixin = null;
	
	public IndexHandlerImpl() {
		super();
		this.ihMixin = new IndexHandlerMixin(this.clusterName, this.ln);
	}
	
	@Override
	public byte[] loadSerializedIndex(long id, String idxTableName) throws TimeSeriesException {
		return ihMixin.loadSerializedIndex(id, idxTableName);
		
		/*
		byte[] serialized = null;
		try {
			IIndex indexService = ServiceFactory.getIndexService();
			serialized = indexService.loadSerializedIndex(this.clusterName, id, idxTableName);
			//indexService.close();
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return serialized;
		*/
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
			IIndex indexService = ServiceFactory.getIndexService();
			indexService.saveSerializedIndex(this.clusterName, this.ln, id, idxTableName, srlIndex, create);
			//indexService.close();
			
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
	}

	@Override
	public Map<Long,Map<IndexType,List<String>>> getIndexList(int ln) throws TimeSeriesException {
		return ihMixin.getIndexList(ln);
		/*
		IConfig configurationService = ServiceFactory.getConfigurationService();
		Map<Long,Map<IndexType,List<String>>> map = configurationService.getIndexList(this.clusterName, ln);
		//configurationService.close();
		return map;
		*/
	}
	
	@Override
	public List<String> parseIndexString(IndexType indexType, String indexString) {
		return ihMixin.parseIndexString(indexType, indexString);
		/*
		if (indexString == null || indexString.length() == 0)
			return null;
		List<String> indexList = new ArrayList<String>(); 
		String[] indexes = indexString.split(";");
		for (String index : indexes) {
			if (index.toLowerCase().startsWith(indexType.name().toLowerCase()))
				indexList.add(index);		
		}
		return indexList;
		*/
	}

	@Override
	public int updateIndex(int ln, String guid, String indexes, boolean drop) throws TimeSeriesException {
		if (indexes == null || indexes.length() == 0)
			return 0;
		IConfig configurationService = ServiceFactory.getConfigurationService();
		IIndex indexService = ServiceFactory.getIndexService();
		
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
					if (!currentIndex.contains(index)) {
						currentIndex += ";" + index;						
					}
				}
				for (MasterData md : list)
					createPatternIndexTables(md.getId(), index, md.getKs());
			}
		}
		List<MasterData> updatedList = new ArrayList<MasterData>();
		for (MasterData md : list) {
			updatedList.add(new MasterData(md.getId(), md.getLn(), md.getGuid(), md.getStartts(), md.getFreq(), 
			md.getDatatype(), currentIndex));
		}
		result = configurationService.updateMasterData(this.clusterName, updatedList);
		//configurationService.close();
		//indexService.close();
		result = list.size();
		return result;
	}
	
	@Override
	public boolean getConfigurationStatus(long id, String index) {
		return ihMixin.getConfigurationStatus(id, index);
		/*
		try {
			IConfig configurationService = ServiceFactory.getConfigurationService();
			boolean status = configurationService.isConfiguredWithIndex(this.clusterName, id, index);
			//configurationService.close();
			return status;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return false;
		*/
	}
	
	@Override
	public List<Integer> getReplicaSet(long id) {
		return ihMixin.getReplicaSet(id);
		
		/*
		try {
			ICoord coordService = ServiceFactory.getCoordinationService();
			List<Integer> result = coordService.getReplicaSet(this.clusterName, id);
			//coordService.close();
			return result;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return null;
		*/
	}
	
	private int deleteIndexRecords(List<MasterData> list, String index, IIndex service) throws TimeSeriesException {
		return ihMixin.deleteIndexRecords(list, index, service);
		
		/*
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return 0;
    	
		for (MasterData md : list) {
			service.deleteSerializedIndex(this.clusterName, md.getId(), index + "_" + md.getId());
		}
		return list.size();
		*/
	}
	
}
