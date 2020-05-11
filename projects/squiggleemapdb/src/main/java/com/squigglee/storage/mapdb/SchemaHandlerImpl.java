// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mapdb;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.TimeSeriesException;

public class SchemaHandlerImpl extends HandlerBase implements ISchemaHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.mapdb.SchemaHandlerImpl");
	protected String createKeyspaceString = "CREATE KEYSPACE IF NOT EXISTS ? WITH REPLICATION = ?";

	@Override
	public boolean createSchema(String schemaName, String replication, String strategy) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return false;
    	
    	throw new TimeSeriesException("This method may not be needed any more");
    	
    	/*
		String[] replicaOptions = replication.split(";");
		String replString = "{ 'class' : '" + strategy + "'";
		for (String replicaOption : replicaOptions) {
			String[] options = replicaOption.split(":");
			replString += " , '" + options[0] + "'" + " : " + options[1];
		}
		replString += "}";
		
		String kscreate = "CREATE KEYSPACE IF NOT EXISTS " + schemaName + " WITH REPLICATION = " + replString;
		System.out.println(kscreate);

		getSession().execute(kscreate);
		return true;
		*/
	}
	
	public boolean delete(long id, String tableName) {
		//if (!_ENGINES.containsKey(id) || !_TABLES.containsKey(id) || !_TABLES.get(id).containsKey(tableName) || !_ENGINES.get(id).exists(tableName))
		//	return false;
		
		//_ENGINES.get(id).delete(tableName);
		//_ENGINES.get(id).commit();
		return true;
	}
	
	@Override
	public void deleteSchema(String keyspaceName) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return;
    	
    	throw new TimeSeriesException("This method may not be needed any more");
    	
		//getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute("DROP KEYSPACE " + keyspaceName + ";");
	}

	@Override
	public void createMasterSchema(String replication, String strategy) throws TimeSeriesException {
		// nothing to do here , configuration service stores master data 
		
		throw new TimeSeriesException("This method may not be needed any more");
	}

	protected boolean keyspaceExists(String ks) {
		return false;
		/*
		try {
			if (getSession(ks) != null)
				return true;
		} catch (Exception e) {
			logger.info("keyspace " + ks + " does NOT exist");
		}
		return false;
		*/
	}
}
