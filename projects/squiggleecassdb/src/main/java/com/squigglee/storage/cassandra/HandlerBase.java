// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.cassandra;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.AvroTimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.AvroTimeSeriesSerializer;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.ITimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.ITimeSeriesSerializer;

public class HandlerBase implements IHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.cassandra.HandlerBase");
	protected Cluster cluster = null;
	protected Map<String,Session> _contexts = new HashMap<String,Session>(); 
	protected String clusterName = null;
	protected int clusterPort;
	boolean isDataNode = false;
	protected String clusterSeeds = null;
	protected String localDataCenter = null;
	protected int ln = 0;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	protected String address = null;
	
	public HandlerBase() {

		try {
			this.clusterName = LocalNodeProperties.getClusterName();
			this.ln = LocalNodeProperties.getNodeLogicalNumber();
			this.clusterPort = LocalNodeProperties.getClusterPort();
			this.clusterSeeds = LocalNodeProperties.getClusterSeeds();
			this.localDataCenter = LocalNodeProperties.getLocalDataCenter();
			this.address = LocalNodeProperties.getNodeAddress();
			isDataNode = LocalNodeProperties.getNodeLogicalNumber() == LocalNodeProperties.isReplicaOf();
			if (LocalNodeProperties.getSerializerType().equals(TsrConstants.HANDLER_SERIALIZER_AVRO)) {
				this.deserializer = new AvroTimeSeriesDeserializer();
				this.serializer = new AvroTimeSeriesSerializer();
			}
		} catch (TimeSeriesException tse) {
			logger.error("Failed to initialize handler", tse);
		}
	}

	protected void initializeContext(String keyspaceName) {
		if (_contexts.get(keyspaceName) != null)
			return;
		Session context = cluster.connect(keyspaceName);
		//System.out.println("Opened session: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass() + " for keyspace = " + keyspaceName);
		//logger.debug("Opened session: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass() + " for keyspace = " + keyspaceName);
		_contexts.put(keyspaceName, context);
	}

	public Session getSession(String keyspaceName) {
		if (_contexts.get(keyspaceName) == null)
			_contexts.put(keyspaceName, cluster.connect(keyspaceName));
		
		return _contexts.get(keyspaceName);
	}
	
	public Session getSession() {
		return cluster.connect();
	}
	
	@Override
	public void initialize() {
		buildCluster();
		initializeContext(TsrConstants.MASTER_DATA_KEYSPACE_NAME);
	}

	@Override
	public void reset(String dataType) throws TimeSeriesException {
		Schema.Type schemaType = DynamicTypeTranslator.getSchemaType(dataType);
		this.serializer.resetSchema(schemaType);
		this.deserializer.resetSchema(schemaType);
	}
	
	@Override
	public void shutdown() {
		//System.out.println("Closing sessions: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass());
		//logger.debug("Closing sessions: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass());
		
		if (_contexts != null) {
			for (String ctxName : _contexts.keySet()) {
				if (_contexts.get(ctxName) != null) {
					try {
						_contexts.get(ctxName).close();
					} catch (Exception ex) {}
				}
			}
			_contexts.clear();
		}
		if (cluster != null)
			cluster.close();
	}
	
	protected void buildCluster() {
		if (cluster == null || cluster.isClosed()) {
			Builder builder = Cluster.builder().withoutJMXReporting().withoutMetrics()
					.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(this.localDataCenter)));
			String[] seeds = clusterSeeds.split(";");
			
			boolean found = false;
			for (String seed : seeds) {
				//builder.addContactPoint(seed);
				builder.addContactPoint(seed.split(":")[0]);
				if (seed.equalsIgnoreCase(this.address))
					found = true;
			}
			
			if (!found)
				builder.addContactPoint(this.address);
			cluster = builder.build();
			//System.out.println("Built cluster: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass());
			//logger.debug("Built cluster: " + cluster.getMetadata().getClusterName() + " for handler " + this.getClass());
		}
	}
}