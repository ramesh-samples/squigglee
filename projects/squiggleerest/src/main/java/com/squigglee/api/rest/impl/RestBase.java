package com.squigglee.api.rest.impl;

import org.apache.log4j.Logger;
import org.jboss.resteasy.plugins.providers.RegisterBuiltin;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import com.squigglee.api.restproxy.ConfigurationProxy;
import com.squigglee.api.restproxy.EventProxy;
import com.squigglee.api.restproxy.IndexSchemaProxy;
import com.squigglee.api.restproxy.OperatorProxy;
import com.squigglee.api.restproxy.PatternProxy;
import com.squigglee.api.restproxy.SynopsesProxy;
import com.squigglee.api.restproxy.TimeSeriesProxy;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public abstract class RestBase {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.RestBase");

	protected static TimeSeriesProxy tsProxy = null;
	protected static PatternProxy patternProxy = null;
	protected static SynopsesProxy synopsesProxy = null;
	protected static OperatorProxy operatorProxy = null;
	protected static ConfigurationProxy configProxy = null;
	protected static IndexSchemaProxy indexSchemaProxy = null;
	protected static EventProxy eventProxy = null;
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
	protected static int eventMax = 10000;	//TODO get from configuration

	static {
		initialize();
	}
	
	private static void initialize() {
		RegisterBuiltin.register(ResteasyProviderFactory.getInstance());
		try {	
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			replicaOf = LocalNodeProperties.isReplicaOf();
			localCluster = LocalNodeProperties.getClusterName();
			dataCenter = LocalNodeProperties.getLocalDataCenter();
			address = LocalNodeProperties.getNodeAddress();
			tsProxy = new TimeSeriesProxy(HandlerFactory.getDataHandler(), localLn, localCluster, limit);
			patternProxy = new PatternProxy(HandlerFactory.getMasterDataHandler(), HandlerFactory.getPatternHandler(), localLn, localCluster);
			synopsesProxy = new SynopsesProxy(HandlerFactory.getMasterDataHandler(), HandlerFactory.getSampledDataHandler(), 
					HandlerFactory.getSketchHandler(), localLn, localCluster, limit);
			operatorProxy = new OperatorProxy(HandlerFactory.getDataHandler(), localLn, localCluster, limit);
			configProxy = new ConfigurationProxy(HandlerFactory.getMasterDataHandler(), localLn, localCluster);
			indexSchemaProxy = new IndexSchemaProxy(HandlerFactory.getDataHandler(), localLn, localCluster);
			eventProxy = new EventProxy(HandlerFactory.getCEPDataHandler(), HandlerFactory.getStatusHandler(), localLn, localCluster, eventMax);
		} catch (TimeSeriesException e) {
			logger.error("Found error instantiating data handlers", e);
		}
	}
	
}
