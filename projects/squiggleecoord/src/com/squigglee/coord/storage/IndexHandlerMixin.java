// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.TimeSeriesException;

public class IndexHandlerMixin extends MasterDataHandlerMixin {
	
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.IndexHandlerImpl");
	
	public byte[] loadSerializedIndex(String cluster, long id, String idxTableName) throws TimeSeriesException {
		byte[] serialized = null;
		try {
			IIndexService indexService = ServiceFactory.getIndexService();
			int localln = indexService.getLogicalNode(id);
			serialized = indexService.loadSerializedIndex(cluster, localln, id, idxTableName);
			//indexService.close();
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return serialized;		
	}

	public Map<Long,Map<IndexType,List<String>>> getIndexList(String cluster, int ln) throws TimeSeriesException {
		IConfigService configurationService = ServiceFactory.getConfigurationService();
		Map<Long,Map<IndexType,List<String>>> map = configurationService.getIndexList(cluster, ln);
		//configurationService.close();
		return map;
	}
	
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
	
	public boolean getConfigurationStatus(String cluster, long id, String index) {
		try {
			IConfigService configurationService = ServiceFactory.getConfigurationService();
			boolean status = configurationService.isConfiguredWithIndex(cluster, id, index);
			//configurationService.close();
			return status;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
		return false;
	}
	
	public List<Integer> getReplicaSet(String cluster, long id) {
		try {
			ICoordService coordService = ServiceFactory.getCoordinationService();
			List<Integer> result = coordService.getReplicaSet(cluster, id);
			return result;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get replica set for id = " + id, e);
		}
		return null;
	}
	
	public List<Integer> getReplicaSet(String cluster, int dataln) {
		try {
			ICoordService coordService = ServiceFactory.getCoordinationService();
			List<Integer> result = coordService.getReplicaSet(cluster, dataln);
			return result;
		} catch (TimeSeriesException e) {
			logger.error("Failed to get replica set for ln = " + dataln, e);
		}
		return null;
	}
}
