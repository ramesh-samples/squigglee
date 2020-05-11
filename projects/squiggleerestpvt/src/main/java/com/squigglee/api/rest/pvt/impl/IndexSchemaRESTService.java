package com.squigglee.api.rest.pvt.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.pvt.IIndexSchemaRESTService;
import com.squigglee.core.entity.TimeSeriesException;

public class IndexSchemaRESTService extends RestBase implements IIndexSchemaRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.pvt.IIndexSchemaRESTService");
	protected static ExecutorService executorService = null;
	
	@Override
	public Boolean deletePatternIndexTablesJSON(String cluster, int ln, List<Long> idList, String indexName) {
		try {
			return indexSchemaProxy.deletePatternIndexTables(cluster, ln, idList, indexName);
		} catch (TimeSeriesException e) {
			logger.error("Found error deleting pattern index tables for index = " + indexName + " and ids = " + idList + " in cluster " + cluster, e);
			return false;
		}
	}
	
	@Override
	public Boolean createPatternIndexTablesJSON(String cluster, int ln, long id, String indexName) {
		try {
			return indexSchemaProxy.createPatternIndexTables(cluster, ln, id, indexName);
		} catch (TimeSeriesException e) {
			logger.error("Found error deleting pattern index tables for index = " + indexName + " and id = " + id + " in cluster " + cluster, e);
			return false;
		}
	}

}
