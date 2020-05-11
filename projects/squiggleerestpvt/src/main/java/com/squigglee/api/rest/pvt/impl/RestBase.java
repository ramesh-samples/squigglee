package com.squigglee.api.rest.pvt.impl;

import org.apache.log4j.Logger;
import org.jboss.resteasy.plugins.providers.RegisterBuiltin;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import com.squigglee.api.rest.pvt.RESTFactory;
import com.squigglee.api.restproxy.IndexSchemaProxy;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.interfaces.IDataHandler;

public abstract class RestBase {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.pvt.RestBase");
	
	protected static HandlerFactory factory = null;
	protected static IDataHandler dataHandler = null;
	protected static IndexSchemaProxy indexSchemaProxy = null;
	protected static int limit = 100000;
	protected static int localLn = 0;
	protected static int replicaOf = 0;
	protected static String localCluster = null;
	protected static String dataCenter = null;
	protected static String path = "/squiggleerestui";
	protected static int port = 8080;
	protected static String transport = "http";
	protected static String address = "127.0.0.1";
	protected static int connectionTimeoutMillis = 20000;
	protected static int socketTimeoutMillis = 20000;
	protected static RESTFactory restFactory = null;
	static {
		initialize();
	}
	
	private static void initialize() {
		RegisterBuiltin.register(ResteasyProviderFactory.getInstance());
		try {	
			factory = new HandlerFactory();
			dataHandler = factory.getNewDataHandler();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			localCluster = LocalNodeProperties.getClusterName();
			dataCenter = LocalNodeProperties.getLocalDataCenter();
			address = LocalNodeProperties.getNodeAddress();
			restFactory = new RESTFactory(path, port, transport, connectionTimeoutMillis, socketTimeoutMillis);
			indexSchemaProxy = new IndexSchemaProxy(dataHandler, localLn, localCluster);
		} catch (TimeSeriesException e) {
			logger.error("Found error instantiating data handler", e);
		}
	}
}
