// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Session;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.TimeSeriesException;

public class SchemaHandlerImpl extends HandlerBase implements ISchemaHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.SchemaHandlerImpl");
	protected String createKeyspaceString = "CREATE KEYSPACE IF NOT EXISTS ? WITH REPLICATION = ?";


	public SchemaHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}

	@Override
	public boolean createSchema(String schemaName, String replication, String strategy) throws TimeSeriesException {
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
		
		String kscreate = "CREATE KEYSPACE IF NOT EXISTS " + schemaName + " WITH REPLICATION = " + replString;
		System.out.println(kscreate);

		getSession().execute(kscreate);
		return true;
	}
	
	@Override
	public void deleteSchema(String keyspaceName) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches
    	if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return;
    	
		getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute("DROP KEYSPACE " + keyspaceName + ";");
	}

	@Override
	public void createMasterSchema(String replication, String strategy) throws TimeSeriesException {
		//best to do table operations only from data nodes to avoid eventual consistency headaches & glitches 
		if (LocalNodeProperties.getNodeLogicalNumber() != LocalNodeProperties.isReplicaOf())
    		return;
		
		if (!keyspaceExists(TsrConstants.MASTER_DATA_KEYSPACE_NAME)) {
			createSchema(TsrConstants.MASTER_DATA_KEYSPACE_NAME, replication, strategy);
			Session mdKs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME);
			String createTableQuery = "CREATE TABLE IF NOT EXISTS " + TsrConstants.MASTER_DATA_CF_NAME + 
						" (id bigint, ks text, ln int, guid text, indexes text, startts bigint, freq text" + 
						", datatype text, replication text, strategy text, PRIMARY KEY (id));";
			mdKs.execute(createTableQuery);
				
			String guidIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME 
				+ "." + TsrConstants.MASTER_DATA_CF_NAME + "(guid);";
			mdKs.execute(guidIndexQuery);
				
			String starttsIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME 
					+ "." + TsrConstants.MASTER_DATA_CF_NAME + "(startts);";
			mdKs.execute(starttsIndexQuery);
	
			String lnIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME 
					+ "." + TsrConstants.MASTER_DATA_CF_NAME + "(ln);";
			mdKs.execute(lnIndexQuery);
				
			String ksIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME 
					+ "." + TsrConstants.MASTER_DATA_CF_NAME + "(ks);";
			mdKs.execute(ksIndexQuery);

			String dataTypeIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME 
					+ "." + TsrConstants.MASTER_DATA_CF_NAME + "(datatype);";
			mdKs.execute(dataTypeIndexQuery);
		}
	}

	protected boolean keyspaceExists(String ks) {
		try {
			if (getSession(ks) != null)
				return true;
		} catch (Exception e) {
			logger.info("keyspace " + ks + " does NOT exist");
		}
		return false;
	}

	@Override
	public boolean delete(long id, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
}
