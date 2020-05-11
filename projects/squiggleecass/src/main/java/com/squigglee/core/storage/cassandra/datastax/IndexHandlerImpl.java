// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.sketch.LocalitySensitiveHasher;

public class IndexHandlerImpl extends MasterDataHandlerImpl implements IIndexHandler {
	
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.IndexHandlerImpl");
	private static Map<String,PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
	private static String insertSerializedString = "insert into srlindx (id, pindx, idxbegin, idxend, srlblob) values (?, ?, ?, ?, ?)";
	//private static String insertSerializedString = "update srlindx set idxbegin = ?, idxend = ?, srlblob= ? where id = ? and pindx = ?";
	public IndexHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}

	@Override
	public byte[] loadSerializedIndex(long id, String idxTableName) throws TimeSeriesException {
		String dataKeyspace = null;
		//int minKey = 0;
		//int maxKey = 0;
		byte[] serialized = null;
		String select = "select id, pindx, idxbegin"  + ", idxend"  + ", srlblob from srlindx where id = " + id + " and pindx = '" + idxTableName + "_" + id + "';";
		//PreparedStatement ps = getSession(dataKeyspace).prepare(select);
		//ResultSet rs = getSession(dataKeyspace).execute(ps.bind(id, idxTableName));
		try {
			ResultSet rs = getSession(dataKeyspace).execute(select);
			Iterator<Row> it = rs.iterator();
			while (it.hasNext()) {		
				Row row = it.next();
				//minKey = row.getInt("idxbegin" + hashNumber);
				//maxKey = row.getInt("idxend"+ hashNumber);
				ByteBuffer bb = row.getBytes("srlblob");
				serialized = new byte[bb.remaining()];
				bb.get(serialized, 0, serialized.length);
			}
		} catch (Exception ex) {
			logger.error("Found error fetching the serialized index", ex);
			throw new TimeSeriesException(ex);
		}
		return serialized;
	}

	@Override
	public void saveSerializedIndex(long id, String idxTableName, byte[] srlIndex, boolean create) {
		String dataKeyspace = null;
		if (!getConfigurationStatus(id,idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + id + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + id + " index = " + idxTableName);
			return;
		}

		String insertString = "insert into srlindx (id, pindx, idxbegin"  + ", idxend"  + ", srlblob) values (?, ?, ?, ?, ?);";
		if (!psMap.containsKey(dataKeyspace)) {
			psMap.put(dataKeyspace, getSession(dataKeyspace).prepare(insertString));
		}
		
		//System.out.println(insertString);
		ResultSet rs = getSession(dataKeyspace).execute(psMap.get(dataKeyspace).bind(id, idxTableName + "_" + id, ByteBuffer.wrap(srlIndex)));
		if (rs != null && rs.wasApplied()) {
			//System.out.println("Successfully saved index " + idxTableName + " for id = " + id);
			logger.debug("Successfully saved index " + idxTableName + " for id = " + id);
		}
		else {
			//System.out.println("Failed to save index " + idxTableName + " for id = " + id);
			logger.debug("Failed to save index " + idxTableName + " for id = " + id);
		}
	}
	
	@Override
	public Map<Long,Map<IndexType,List<String>>> getIndexList(int ln) throws TimeSeriesException {
		Map<Long,Map<IndexType,List<String>>> map = new HashMap<Long,Map<IndexType,List<String>>>();
		//example "ptrn_16_1000_100_8_1000;skchCM_1_3599999_1024_50_1000"
		//List<String> indexes = new ArrayList<String>();
		String cql = "select id,indexes from " + TsrConstants.MASTER_DATA_CF_NAME + 
				" WHERE ln = " + ln + " ALLOW FILTERING;";
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			String index = row.getString("indexes");
			if (index == null || index.length() == 0)
				continue;
			long id = row.getLong("id");
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
		int result = 0;
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE LN = " + ln + " AND guid = '" + guid + "' ALLOW FILTERING;";
		List<MasterData> list = new ArrayList<MasterData>();
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			list.add( new MasterData (
					row.getLong("id"),
					row.getString("ks"),
					row.getInt("ln"),
					row.getString("guid"),
					row.getLong("startts"),
					Frequency.valueOf(row.getString("freq")),
					row.getString("datatype"),
					row.getString("replication"),
					row.getString("strategy"),
					row.getString("indexes")
				));
		}
		if (list.size() == 0)
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
				deleteIndexRecords(list, index);
				if (index.toLowerCase().contains("ptrn"))
					deleteIndexTables(list,index);
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
		String inList = "(";
		for (int i=0; i< list.size(); i++) {
			inList += ( (i==0)?"":",") + list.get(i).getId();
		}
		inList += ")";
		String updateCql = "update " + TsrConstants.MASTER_DATA_CF_NAME + " SET indexes = '" + currentIndex + "' WHERE id in " + inList;
		
		ResultSet rsUpdate = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(updateCql);
		if (rsUpdate != null && rsUpdate.wasApplied()) {
			System.out.println( ((drop)?"Deleted":"Added") + " index " + indexes + " for node = " + ln + " and parameter = " + guid);
			logger.debug( ((drop)?"Deleted":"Added") + " index " + indexes + " for node = " + ln + " and parameter = " + guid);
		}
		
		result = list.size();
		return result;
	}
	
	@Override
	public boolean getConfigurationStatus(long id, String index) {
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE ID = " + id + ";";
		List<MasterData> list = new ArrayList<MasterData>();
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			list.add( new MasterData (
					row.getLong("id"),
					row.getString("ks"),
					row.getInt("ln"),
					row.getString("guid"),
					row.getLong("startts"),
					Frequency.valueOf(row.getString("freq")),
					row.getString("datatype"),
					row.getString("replication"),
					row.getString("strategy"),
					row.getString("indexes")
				));
		}
		if (list.size() == 0)
			return false;
		String currentIndex = list.get(0).getIndexes();
		if (currentIndex != null && currentIndex.toLowerCase().contains(index.toLowerCase()))
			return true;
		
		return false;
	}
	
	@Override
	public List<Integer> getReplicaSet(long id) {
		List<Integer> replicaSet = new ArrayList<Integer>();
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE ID = " + id + ";";
		List<MasterData> list = new ArrayList<MasterData>();
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			list.add( new MasterData (
					row.getLong("id"),
					row.getString("ks"),
					row.getInt("ln"),
					row.getString("guid"),
					row.getLong("startts"),
					Frequency.valueOf(row.getString("freq")),
					row.getString("datatype"),
					row.getString("replication"),
					row.getString("strategy"),
					row.getString("indexes")
				));
		}
		
		if (list.size() == 0 || list.get(0) == null || list.get(0).getReplication() == null)
			return replicaSet;
			//return 1;
		String replication = list.get(0).getReplication();
		String[] tokens = replication.split(";");
		int count = 0;
		for (String token : tokens) {
			if (token.toLowerCase().contains("class"))
				continue;
			String[] vals = token.split(":");
			count += Integer.parseInt(vals[1]);
		}
		//return (count == 0?1:count);
		return replicaSet;
	}
	
	private int deleteIndexRecords(List<MasterData> list, String index) throws TimeSeriesException {
		
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return 0;
    	
		int successCount = 0;
		for (MasterData md : list) {
			String delCql = "delete from srlindx where id = " + md.getId() + " and pindx = '"+ index + "_" + md.getId() + "';";
			SimpleStatement st = new SimpleStatement(delCql);
			st.setConsistencyLevel(ConsistencyLevel.ONE);
			ResultSet rsUpdate = getSession(md.getKs()).execute(st);
			if (rsUpdate != null && rsUpdate.wasApplied()) {
				System.out.println("Deleted index record from srlindx table for index = " + index + "_" + md.getId());
				logger.debug("Deleted index record from srlindx table for index = " + index + "_" + md.getId());
				successCount++;
			} else
			{
				System.out.println("Failed to delete index record from srlindx table for index = " + index + "_" + md.getId());
				logger.debug("Failed to delete index record from srlindx table for index = " + index + "_" + md.getId());
			}
		}
		return successCount;
	}
	
	private int deleteIndexTables(List<MasterData> list, String index) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return 0;
    	
		int successCount = 0;
		for (MasterData md : list) {
			int projections = LocalitySensitiveHasher.getConfiguredIndexProjections(index);
			for ( int proj = 0; proj < projections; proj++) {
				String delCql = "drop table if exists "+ index + "_" + md.getId() + "_" + proj + ";";
				SimpleStatement st = new SimpleStatement(delCql);
				st.setConsistencyLevel(ConsistencyLevel.ONE);
				ResultSet rsUpdate = getSession(md.getKs()).execute(st);
				if (rsUpdate != null && rsUpdate.wasApplied()) {
					System.out.println("Deleted index table " + index + "_" + md.getId() + "_" + proj);
					logger.debug("Deleted index table " + index + "_" + md.getId() + "_" + proj);
					successCount++;
				} else
				{
					System.out.println("Failed to delete index table " + index + "_" + md.getId() + "_" + proj);
					logger.debug("Failed to delete index table " + index + "_" + md.getId() + "_" + proj);
				}
			}
		}
		return successCount;
	}
	
}
