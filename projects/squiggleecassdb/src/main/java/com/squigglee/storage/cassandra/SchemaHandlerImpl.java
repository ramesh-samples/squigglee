// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.cassandra;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.sketch.LocalitySensitiveHasher;

public class SchemaHandlerImpl extends HandlerBase implements ISchemaHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.cassandra.SchemaHandlerImpl");
	protected String createKeyspaceString = "CREATE KEYSPACE IF NOT EXISTS ? WITH REPLICATION = ?";
	protected String createTableQueryTemplate = "CREATE TABLE IF NOT EXISTS " + TsrConstants.DATA_CF_NAME + 
			" (id bigint, offset bigint, val %s, PRIMARY KEY (id,offset));";

	@Override
	public boolean createSchema(MasterData md) throws TimeSeriesException {
		String replication = "replication_factor:1";
		String strategy = "SimpleStrategy";
		
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return false;
    	
		String[] replicaOptions = replication.split(";");
		String replString = "{ 'class' : '" + strategy + "'";
		for (String replicaOption : replicaOptions) {
			String[] options = replicaOption.split(":");
			replString += " , '" + options[0] + "'" + " : " + options[1];
		}
		replString += "}";
		
		String kscreate = "CREATE KEYSPACE IF NOT EXISTS " + md.getKs() + " WITH REPLICATION = " + replString;
		//System.out.println(kscreate);
		getSession().execute(kscreate);
		String createTableQuery = String.format(createTableQueryTemplate, MasterData.parseDatatypeFromKs(md.getKs()));
		getSession(md.getKs()).execute(createTableQuery);
		return true;
	}
	
	@Override
	public boolean createSchema(MasterData md, int offsetCount) throws TimeSeriesException {
		return createSchema(md);
	}
	
	@Override
	public void deleteSchema(long id) throws TimeSeriesException {
		//TODO RKR implement this 
		//getSession(md.getKs()).execute("DELETE FROM " + TsrConstants.DATA_CF_NAME + " WHERE id = " + md.getId() + ";");
	}

	@Override
	public boolean deletePatternIndexTables(List<MasterData> list, String index) throws TimeSeriesException {
    	boolean result = false;
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
					result = true;
				} else
				{
					System.out.println("Failed to delete index table " + index + "_" + md.getId() + "_" + proj);
					logger.debug("Failed to delete index table " + index + "_" + md.getId() + "_" + proj);
				}
			}
		}
		return result;
	}
	
	@Override
	public boolean createPatternIndexTables(long id, String idxTableName, String dataKeyspace) {
		boolean result = false;
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
					result = true;
				}
			}
		}
		return result;
	}
}
