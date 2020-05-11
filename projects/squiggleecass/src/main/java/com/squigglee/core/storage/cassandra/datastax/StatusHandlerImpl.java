// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.core.interfaces.NodeStatus;
import com.squigglee.core.interfaces.TimeSeriesException;

public class StatusHandlerImpl extends SchemaHandlerImpl implements IStatusHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.StatusHandlerImpl");

	protected static int TTL = 60; // Status time to live in seconds 
	
	public StatusHandlerImpl (String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}

	//call on create request
	@Override
	public void updateNode(int ln, String addr, String dataCenter, String instanceId, String name, boolean isBoot, boolean isSeed, 
			int replicaOf, int storage, String stype) throws TimeSeriesException {
		
			String update = "INSERT INTO NODESTATUS (ln, addr, dc, iid, nm, isBoot, isSeed, replicaOf, storage, stype) VALUES (" 
			+ ln + ", '" + addr + "', '" + dataCenter + "', '" + instanceId + "', '" + name + "', " 
					+ (isBoot?"True":"False") + ", " + (isSeed?"True":"False") + ", " +
					+ replicaOf + ", " + storage + ", '" + stype + "')";
			
			
			logger.debug(update);
			ResultSet inserted = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(update);
			if (inserted != null && inserted.wasApplied())
				logger.debug("Update node status for node = " + ln);
	}
	
	@Override
	public void updateIndexServiceStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,"isStat");
	}

	@Override
	public void updateOverlayServiceStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,"osStat");
	}

	@Override
	public void updateViewStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,"vwStat");
	}

	@Override
	public void updateGlobalViewStatus(int ln) throws TimeSeriesException {
		updateStatus(ln,"glStat");
	}
	
	private void updateStatus(int ln, String stat) throws TimeSeriesException {
		String update = "INSERT INTO NODESTATUS (ln, stStat, " + stat + ") VALUES (" + ln + ", true, true) using TTL " + TTL + ";";
		logger.debug(update);
		ResultSet inserted = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(update);
		if (inserted != null && inserted.wasApplied())
			logger.debug("Update node status for node = " + ln);
	}
	

	@Override
	public List<NodeStatus> fetchClusterStatus() throws TimeSeriesException {
		SortedMap<Integer,NodeStatus> map = new TreeMap<Integer,NodeStatus>();
		//List<NodeStatus> list = new ArrayList<NodeStatus>();
		String query = "SELECT ln,addr,dc,iid,nm,isboot,isseed,replicaof,storage,stype,ststat,isstat,osstat,vwstat,glstat FROM NODESTATUS ALLOW FILTERING";
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(query);
		Iterator<Row> it = rs.iterator();
		String cluster = LocalNodeProperties.getClusterName();
		NodeStatus ns = null;
		while (it.hasNext()) {		
			Row row = it.next();
			ns = new NodeStatus();
			ns.setLogicalNumber(row.getInt("ln"));
			ns.setAddress(row.getString("addr"));
			ns.setDataCenter(row.getString("dc"));
			ns.setInstanceId(row.getString("iid"));
			ns.setName(row.getString("nm"));
			ns.setBootstrapNode(row.getBool("isboot"));
			ns.setSeedNode(row.getBool("isseed"));
			ns.setReplicaOf(row.getInt("replicaof"));
			ns.setStorage(row.getInt("storage"));
			ns.setStype(row.getString("stype"));
			ns.setStorageServiceUp(row.getBool("ststat"));
			ns.setIndexServiceUp(row.getBool("isstat"));
			ns.setOverlayServiceUp(row.getBool("osstat"));
			ns.setViewUp(row.getBool("vwstat"));
			ns.setGlobalViewUp(row.getBool("glstat"));
			ns.setCluster(cluster);
			//list.add(ns);
			map.put(row.getInt("ln"), ns);
		}
		return new ArrayList<NodeStatus>(map.values());
	}
	
	@Override
	public void initialize() {
		super.initialize();
		createStatusTable();
	}
	
	protected void createStatusTable() {
		String query = "CREATE TABLE IF NOT EXISTS NODESTATUS (ln int, addr text, dc text, iid text, nm text, " + 
				"isboot boolean, isseed boolean, replicaof int, storage int, stype text, " + 
				"ststat boolean, isstat boolean, osstat boolean, vwstat boolean, glstat boolean, " + 
				"PRIMARY KEY (ln));";			
		
		//ResultSet rs = 
				getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(query);
		//if (rs.wasApplied())
		//	logger.debug("Created node status table on keyspace " + TsrConstants.MASTER_DATA_KEYSPACE_NAME);
			//System.out.println(query2Result);
	}

	@Override
	public void deleteNode(int ln) throws TimeSeriesException {
			String query = "DELETE FROM NODESTATUS WHERE LN = " + ln;
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(query);
			if (rs.wasApplied())
				logger.debug("Deleted node status entry for node = " + ln);
	}

}
