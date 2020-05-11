// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IOverlayHandler;
import com.squigglee.core.interfaces.TimeSeriesException;


public class OverlayHandlerImpl extends MasterDataHandlerImpl implements IOverlayHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.OverlayHandlerImpl");
	
	public OverlayHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public Map<String, Map<Integer, List<String>>> getOverlayNetwork() throws TimeSeriesException {
		Map<String,Map<Integer, List<String>>> overlayNetwork = new HashMap<String,Map<Integer, List<String>>>();
		String cql = "select id,ln,guid,datatype from " + TsrConstants.MASTER_DATA_CF_NAME + " ALLOW FILTERING;";
		try {
			//long rowId = -1; 
			int ln = -1; 
			String guid = null;
			String dataType = null;
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
			Iterator<Row> it = rs.iterator();
			while (it.hasNext()) {		
				Row row = it.next();		
				//rowId = row.getLong("id");
				ln = row.getInt("ln");
				guid = row.getString("guid");
				dataType = row.getString("datatype");
				updateNetwork(overlayNetwork, dataType, ln, guid);
			}
		} catch (Exception ex) {
			logger.error("Error creating overlay network ",ex);
			//throw new TimeSeriesException(ex);
			return null;
		}
		return overlayNetwork;
	}
	
	private void updateNetwork(Map<String,Map<Integer, List<String>>> overlayNetwork, String dataType, int ln, String guid) {
		if (!overlayNetwork.containsKey(dataType))
			overlayNetwork.put(dataType, new HashMap<Integer,List<String>>());
		if (!overlayNetwork.get(dataType).containsKey(ln))
			overlayNetwork.get(dataType).put(ln, new ArrayList<String>());
		if (!overlayNetwork.get(dataType).get(ln).contains(guid))
			overlayNetwork.get(dataType).get(ln).add(guid);
	}

}
