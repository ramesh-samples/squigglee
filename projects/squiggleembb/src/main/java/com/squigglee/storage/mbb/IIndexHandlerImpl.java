// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.storage.IndexHandlerMixin;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IIndexHandler;

public class IIndexHandlerImpl extends IDataHandlerImpl implements IIndexHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.IIndexHandlerImpl");
	private IndexHandlerMixin ihMixin = null;
	
	@Override
	public void initialize() {
		super.initialize();
		this.ihMixin = new IndexHandlerMixin();
	}
	
	@Override
	public byte[] loadSerializedIndex(String cluster, long id, String idxTableName) throws TimeSeriesException {
		return ihMixin.loadSerializedIndex(cluster, id, idxTableName);
	}

	@Override
	public void saveSerializedIndex(String cluster, long id, String idxTableName, byte[] srlIndex, boolean create) {
		if (srlIndex == null)
			return;
		
		if (!getConfigurationStatus(cluster, id,idxTableName)) {
			System.out.println("Index not (no longer) longer configured, skipping update for id = " + id + " index = " + idxTableName);
			logger.debug("Index not (no longer) configured, skipping update for id = " + id + " index = " + idxTableName);
			return;
		}
		try {
			IIndexService indexService = ServiceFactory.getIndexService();
			int ln = indexService.getLogicalNode(id);
			indexService.saveSerializedIndex(cluster, ln, id, idxTableName, srlIndex, create);			
		} catch (TimeSeriesException e) {
			logger.error("Failed to get configuration service from service factory", e);
		}
	}

	@Override
	public Map<Long,Map<IndexType,List<String>>> getIndexList(String cluster, int ln) throws TimeSeriesException {
		return ihMixin.getIndexList(cluster, ln);
	}
	
	@Override
	public List<String> parseIndexString(IndexType indexType, String indexString) {
		return ihMixin.parseIndexString(indexType, indexString);
	}
	
	@Override
	public boolean getConfigurationStatus(String cluster, long id, String index) {
		return ihMixin.getConfigurationStatus(cluster, id, index);
	}
	
}
