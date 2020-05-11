// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mortbay.log.Log;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesConfig;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.sketch.LocalitySensitiveHasher;

public class MasterDataHandlerImpl extends SchemaHandlerImpl implements IMasterDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.MasterDataHandlerImpl");
	protected String createTableQueryTemplate = "CREATE TABLE IF NOT EXISTS " + TsrConstants.DATA_CF_NAME + 
			" (id bigint, offset bigint, val %s, PRIMARY KEY (id,offset));";
	protected String noCacheCreateTableQueryTemplate = "CREATE TABLE IF NOT EXISTS " + TsrConstants.DATA_CF_NAME + 
			" (id bigint, offset bigint, val %s, PRIMARY KEY (id,offset)) with caching = 'none' ;";
	protected String createIndexTableQuery = "CREATE TABLE IF NOT EXISTS " + TsrConstants.INDEX_CF_NAME + 
			" (id bigint, pindx text, idxbegin0 int, idxend0 int, srlblob blob, idxbegin1 int, idxend1 int, idxbegin2 int, idxend2 int, idxbegin3 int," + 
			" idxend3 int, idxbegin4 int, idxend4 int, idxbegin5 int, idxend5 int, idxbegin6 int, idxend6 int, idxbegin7 int, idxend7 int," + 
			" idxbegin8 int, idxend8 int, idxbegin9 int, idxend9 int, PRIMARY KEY (id,pindx));";
	protected String compactionStrategyUpdate = "ALTER TABLE " + TsrConstants.DATA_CF_NAME + 
			" WITH  compaction = { 'class' :  'LeveledCompactionStrategy'  }";
	protected String compactionStrategyUpdateIndex = "ALTER TABLE " + TsrConstants.INDEX_CF_NAME + " WITH  compaction = { 'class' :  'LeveledCompactionStrategy'  }";
	
	public MasterDataHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public MasterData getMasterData(int ln, String guid, long startts) {
		
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE guid = '" + guid 
				+ "' and ln = " + ln + " and startts <= " 
				+ startts + " ALLOW FILTERING;";
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		long max = 0L;
		Row maxRow = null;
		while (it.hasNext()) {		
			Row row = it.next();
			//ColumnDefinitions cd = row.getColumnDefinitions();
			if (row.getLong("id") > max) {
				max = row.getLong("id");
				maxRow = row;
			}
		}
		if (maxRow == null)
			return null;
		else
			return new MasterData (
				maxRow.getLong("id"),
				maxRow.getString("ks"),
				maxRow.getInt("ln"),
				maxRow.getString("guid"),
				maxRow.getLong("startts"),
				Frequency.valueOf(maxRow.getString("freq")),
				maxRow.getString("datatype"),
				maxRow.getString("replication"),
				maxRow.getString("strategy"),
				maxRow.getString("indexes")
			);
	}

	@Override
	public List<TimeSeriesConfig> getMasterData(int ln, String guid)
			throws TimeSeriesException {
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE guid = '" 
			+ guid + "' AND LN = " + ln + " ALLOW FILTERING;";
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
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
		
		TimeSeriesConfig config = null;
		DateTime storagestart = null;
		DateTime storageend = null;
		
		for (MasterData md : list) {		
			storagestart = new DateTime( md.getStartts(), DateTimeZone.UTC);
			storageend = TimeSeriesShard.getMaxEndDate(md.getFreq(), storagestart);
			config = new TimeSeriesConfig(guid, md.getLn(), md.getReplication(), md.getStrategy(), md.getFreq(), md.getDatatype(),
				md.getIndexes(), storagestart, storageend);
			configs.add(config);
		}
		if (configs == null || configs.isEmpty())
			Log.debug("No master data configured for id =" + guid + " and ln = " + ln);
		
		return configs;
	}

	@Override
	public List<TimeSeriesConfig> getNodeMasterData(int ln)
			throws TimeSeriesException {
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE LN = " + ln + " ALLOW FILTERING;";
		List<TimeSeriesConfig> configs = new ArrayList<TimeSeriesConfig>();
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
		
		TimeSeriesConfig config = null;
		DateTime storagestart = null;
		DateTime storageend = null;
		
		for (MasterData md : list) {		
			storagestart = new DateTime( md.getStartts(), DateTimeZone.UTC);
			//double frequency = md.getFreq();
			//double tickMultiplier = TimeSeriesShard.getTickMultiplier(frequency);
			//long maxOffset = TimeSeriesShard.getMaxoffset(md.getFreq());
			//storageend = new DateTime( md.getStartts() + (long) (maxOffset / tickMultiplier), DateTimeZone.UTC );
			storageend = TimeSeriesShard.getMaxEndDate(md.getFreq(), storagestart);
			config = new TimeSeriesConfig(md.getGuid(), md.getLn(), md.getReplication(), md.getStrategy(), md.getFreq(), md.getDatatype(),
				md.getIndexes(), storagestart, storageend);
			configs.add(config);
		}
		if (configs == null || configs.isEmpty())
			Log.debug("No master data configured for ln = " + ln);
		
		return TimeSeriesConfig.collapseTimeIntervals(configs);
	}
	
	@Override
	public List<MasterData> getMasterData(long id) throws TimeSeriesException {
		String cql = "select * from " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE id = " + id + " ALLOW FILTERING;";
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
		return list;
	}

	@Override
	public List<MasterData> getMasterData(int ln, String guid, long startts, long endts) throws TimeSeriesException {
		
		String cql = "select id,ks,ln,guid,startts,freq,datatype,replication,strategy,indexes from " 
				+ TsrConstants.MASTER_DATA_CF_NAME + " WHERE guid = '" + guid 
				+ "' AND ln = " + ln + " and startts <= " 
				+ endts + " AND startts > " + startts + " ALLOW FILTERING;";
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
		
		MasterData mdBelow = getMasterData(ln, guid, startts);
		if (mdBelow != null) {
			List<Long> shardStartTimestamps = TimeSeriesShard.getShardStartTimestamps(mdBelow.getFreq(), startts, endts);
			if (shardStartTimestamps.contains(mdBelow.getStartts()))
				list.add(mdBelow);
		}
		return list;
	}

	@Override
	public int[] createMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		
		List<Long> list = TimeSeriesShard.getShardStartTimestamps(config.getFrequency(), config.getStartDate(), config.getEndDate());
		int count = 0;
		
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return new int[]{count};

		for (long startts : list) {
			long masterDataId = getNextMasterDataId(config, startts);
			
			String insert = "insert into " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." +
						TsrConstants.MASTER_DATA_CF_NAME + " (id,ks,ln,guid,startts,freq,datatype,replication,strategy,indexes)" + 
			 			" values ("
						+ (masterDataId) + ",'" + config.getKeyspace() + "'," + config.getLogicalNode() + ", " 
			 			+ "'" + config.getGuid() + "'," + startts + ",'" + 
						config.getFrequency() + "','" + config.getDatatype() 
						+ "', '" + config.getReplication() + "' , '" + config.getReplicationStrategy() + "' " 
						+ ", '" + config.getIndexes() + "'" + " );";
			
			SimpleStatement st = new SimpleStatement(insert);
			st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
			ResultSet inserted = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(st);
			if (inserted != null && inserted.wasApplied())
				count++;
			//System.out.println("Inserted master data record :" + masterDataId);
		}
		return new int[]{count};
	}

	@Override
	public List<MasterData> getMasterDataForBlocks(int ln, String guid, long startts, long endts) throws TimeSeriesException {
		List<MasterData> masterDataList = new ArrayList<MasterData>();
		
		MasterData md = getMasterData(ln, guid, startts);
		if (md == null)
			return masterDataList;
		
		List<Long> shardStartTimestamps = TimeSeriesShard.getShardStartTimestamps(md.getFreq(), startts, endts);
		for (long st : shardStartTimestamps)
			masterDataList.add(getMasterData(ln,guid,st));
		return masterDataList;
	}

	private long getNextMasterDataId(TimeSeriesConfig config, long startts) throws TimeSeriesException {
		//System.out.println("*********************** = " + LocalNodeProperties.getClusterSeeds() + " -- " + (LocalNodeProperties.getClusterSeeds() == "127.0.0.1:9160"));
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		throw new TimeSeriesException("Configuration operations permitted only from data nodes");
    	
		String cql = "select id,guid,ks,startts from " + TsrConstants.MASTER_DATA_CF_NAME + " ALLOW FILTERING;";
		try {
			if (!keyspaceExists(config.getKeyspace())) {
				
				if (createSchema(config.getKeyspace(), config.getReplication(), config.getReplicationStrategy())) {
					initializeContext(config.getKeyspace());
					
					Schema.Type dataType = DynamicTypeTranslator.getSchemaType(config.getDatatype());
					String cqlType = DynamicTypeTranslator.getCQLDataType(dataType);
					String createTableQuery = String.format(createTableQueryTemplate, cqlType);
					if (dataType == Schema.Type.BYTES)
						createTableQuery = String.format(noCacheCreateTableQueryTemplate, cqlType);
					SimpleStatement st = new SimpleStatement(createTableQuery);
					st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
					getSession(config.getKeyspace()).execute(st);
					st = new SimpleStatement(compactionStrategyUpdate);
					st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
					getSession(config.getKeyspace()).execute(st);
					st = new SimpleStatement(createIndexTableQuery);
					st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
					getSession(config.getKeyspace()).execute(st);
					st = new SimpleStatement(compactionStrategyUpdateIndex);
					st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
					getSession(config.getKeyspace()).execute(st);
				}
			}
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
			Iterator<Row> it = rs.iterator();
			long max = config.getStartToken();
			//long max = 0L;
			while (it.hasNext()) {		
				Row row = it.next();
				//ColumnList<String> colList = row.getColumns();
				long rowId = row.getLong("id");
				String guid = row.getString("guid");
				String ksName = row.getString("ks");
				long mdstartts = row.getLong("startts");
				if (config.getGuid().equalsIgnoreCase(guid) && config.getKeyspace().equalsIgnoreCase(ksName) && startts == mdstartts)
					return rowId;	// row already exists 
				else
					if (rowId > max && rowId <= config.getEndToken())
						max = rowId;
			}
			return (max+1L);
		} catch (Exception ex) {
			logger.error("Found error fetching max id for data keyspace = " + config.getKeyspace(),ex);
			throw new TimeSeriesException(ex);
		}
	}

	@Override
	public int[] updateMasterData(TimeSeriesConfig config) throws TimeSeriesException {
		//no difference with Cassandra for insert vs. update 
		return createMasterData(config);
	}

	@Override
	public int[] deleteMasterData(TimeSeriesConfig config)
			throws TimeSeriesException {
		int count = 0;
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return new int[]{count};
    	
		List<MasterData> mdList = getMasterData(config.getLogicalNode(), config.getGuid(), config.getStartDate().getMillis(), 
				config.getEndDate().getMillis());
		for (MasterData md : mdList) {
			String delete = "delete from " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." +
						TsrConstants.MASTER_DATA_CF_NAME + " where id = " + md.getId(); 
			SimpleStatement st = new SimpleStatement(delete);
			st.setConsistencyLevel( ((LocalNodeProperties.getClusterSeeds().equals("127.0.0.1:9160")?ConsistencyLevel.ONE:ConsistencyLevel.TWO)) );
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(st);
			if (rs != null && rs.wasApplied())
				count++;
		}
		return new int[]{count};
	}

	@Override
	public int[] getDataStatus(long id, String dataKeyspace) throws TimeSeriesException {
		int lastoffset = -1;
		String select = "select offset from " + TsrConstants.DATA_CF_NAME  + " where id = " + id + " order by offset desc limit 1";
		ResultSet rs = getSession(dataKeyspace).execute(select);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			lastoffset = (int) row.getLong("offset");
			break;
		}
		int firstoffset = -1;
		select = "select offset from " + TsrConstants.DATA_CF_NAME  + " where id = " + id + " order by offset asc limit 1";
		rs = getSession(dataKeyspace).execute(select);
		it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			firstoffset = (int) row.getLong("offset");
			break;
		}
		return new int[]{firstoffset,lastoffset};
	}
	
	
	public void createPatternIndexTables(long id, String idxTableName, String dataKeyspace) {
		if (idxTableName.toLowerCase().contains("ptrn")) {
			//String create2 = "CREATE TABLE IF NOT EXISTS " + idxTableName + "_" + id + " (id bigint, idx text, val int, dummy int, PRIMARY KEY ((id), idx, val) );";
			int hashCount = LocalitySensitiveHasher.getConfiguredIndexSize(idxTableName);
			int projections = LocalitySensitiveHasher.getConfiguredIndexProjections(idxTableName);
			for (int proj = 0; proj < projections; proj++) {
				String create = "CREATE TABLE IF NOT EXISTS " + idxTableName + "_" + id + "_" + proj + " (id bigint, ";
				for (int i=1; i<= hashCount; i++)
					create += "k" + i + " int, ";
				create += "val int, dummy int, PRIMARY KEY ( (id), ";
				for (int i=1; i<= hashCount; i++)
					create += ("k" + i + ",");
				create += "val));";

				ResultSet rsCreate = getSession(dataKeyspace).execute(create);
				if (rsCreate != null && rsCreate.wasApplied()) {
					String compactionStrategyUpdate = "ALTER TABLE " + idxTableName + "_" + id + "_" + proj + " WITH  compaction = { 'class' :  'LeveledCompactionStrategy'  }";
					getSession(dataKeyspace).execute(compactionStrategyUpdate);
					//System.out.println("Created the " + idxTableName + " table");
					logger.debug("Created the " + idxTableName + " table");
				}
			}
		}
	}
	
	@Override
	public int deletePatternIndexTables(List<MasterData> list, String index) throws TimeSeriesException {
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
