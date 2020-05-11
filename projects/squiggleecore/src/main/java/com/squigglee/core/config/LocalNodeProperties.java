// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.squigglee.core.entity.TimeSeriesException;

public class LocalNodeProperties {
	private static Logger logger = Logger.getLogger("com.squigglee.core.config.LocalNodeProperties");
	private static Properties props = null;
	
	static {
		loadPropertiesFile();
	}

	public static String getClusterName() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLUSTER_NAME) != null)
			return props.getProperty(TsrConstants.CLUSTER_NAME);
		throw new TimeSeriesException("Cluster Name is not found in properties file");
	}
	
	public static int getSketchChunkSize() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SKETCH_CHUNK_SIZE) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.SKETCH_CHUNK_SIZE));
		throw new TimeSeriesException("Sketch Chunk Size is not found in properties file");
	}
	
	public static int getSketchNumChunks() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SKETCH_NUM_CHUNKS) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.SKETCH_NUM_CHUNKS));
		throw new TimeSeriesException("Number of Sketch Chunks is not found in properties file");
	}
	
	public static int getIndexChunkSize() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.INDEX_CHUNK_SIZE) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.INDEX_CHUNK_SIZE));
		throw new TimeSeriesException("Index Chunk Size is not found in properties file");
	}
	
	public static int getIndexNumChunks() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.INDEX_NUM_CHUNKS) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.INDEX_NUM_CHUNKS));
		throw new TimeSeriesException("Number of Index Chunks is not found in properties file");
	}

	public static String getSerializerType() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.HANDLER_SERIALIZER) != null)
			return props.getProperty(TsrConstants.HANDLER_SERIALIZER);
		throw new TimeSeriesException("Serializer Type is not found in properties file");
	}
	
	public static int getNodeLogicalNumber() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_LOGICAL_NUMBER) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.NODE_LOGICAL_NUMBER));
		throw new TimeSeriesException("Node Logical Number is not found in properties file");
	}
	
	public static int getOverlayTimerInterval() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.OVERLAY_TIMER_INTERVAL) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.OVERLAY_TIMER_INTERVAL));
		throw new TimeSeriesException("Overlay Timer Interval is not found in properties file");
	}

	public static String getOverlayVdbFile() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.OVERLAY_VDB_FILE) != null)
			return props.getProperty(TsrConstants.OVERLAY_VDB_FILE);
		throw new TimeSeriesException("overlay VDB File Name is not found in properties file");
	}
	
	public static int getPingTimerInterval() {
		if (props != null && props.getProperty(TsrConstants.PING_TIMER_INTERVAL) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.PING_TIMER_INTERVAL));
		return 20;
	}
	
	public static int getConfigurationServiceInterval() {
		if (props != null && props.getProperty(TsrConstants.CONFIGURATION_SERVICE_INTERVAL) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CONFIGURATION_SERVICE_INTERVAL));
		return 60;
	}
	
	public static String getNodeAddress() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_ADDRESS) != null)
			return props.getProperty(TsrConstants.NODE_ADDRESS);
		throw new TimeSeriesException("Node Address is not found in properties file");
	}
	
	public static String getInstanceId() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.INSTANCE_ID) != null)
			return props.getProperty(TsrConstants.INSTANCE_ID);
		throw new TimeSeriesException("Instance Id is not found in properties file");
	}
	
	public static String getNodeDataCenter() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_DATA_CENTER) != null)
			return props.getProperty(TsrConstants.NODE_DATA_CENTER);
		throw new TimeSeriesException("Data Center is not found in properties file");
	}
	
	public static String getNodeName() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_NAME) != null)
			return props.getProperty(TsrConstants.NODE_NAME);
		throw new TimeSeriesException("Node Name is not found in properties file");
	}

	public static boolean isBoostrapNode() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.IS_BOOTSTRAP_SERVER) != null)
			return props.getProperty(TsrConstants.IS_BOOTSTRAP_SERVER).equalsIgnoreCase("yes")?true:false;
		throw new TimeSeriesException("Whether Bootstrap Node is not found in properties file");
	}
	
	public static boolean isSeedNode() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.IS_SEED_SERVER) != null)
			return props.getProperty(TsrConstants.IS_SEED_SERVER).equalsIgnoreCase("yes")?true:false;
		throw new TimeSeriesException("Whether seed server is not found in properties file");
	}
	
	public static String getScriptLocation() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SCRIPT_LOCATION) != null)
			return props.getProperty(TsrConstants.SCRIPT_LOCATION);
		throw new TimeSeriesException("Script Location is not found in properties file");
	}
	
	public static String getAnsibleExe() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.ANSIBLE_EXE) != null)
			return props.getProperty(TsrConstants.ANSIBLE_EXE);
		throw new TimeSeriesException("Ansible Playbook Exe Location is not found in properties file");
	}

	public static int isReplicaOf() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_IS_REPLICA_OF) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.NODE_IS_REPLICA_OF));
		throw new TimeSeriesException("Node Is Replica Of is not found in properties file");
	}
	
	public static int getServerStorage() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_DATA_STORAGE) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.NODE_DATA_STORAGE));
		throw new TimeSeriesException("Node Data Storage is not found in properties file");
	}
	
	public static String getServerType() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.NODE_SERVER_TYPE) != null)
			return props.getProperty(TsrConstants.NODE_SERVER_TYPE);
		throw new TimeSeriesException("Node Server Type is not found in properties file");
	}
	
	public static int getMaxMatchCandidates() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.MAX_MATCH_CANDIDATES) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.MAX_MATCH_CANDIDATES));
		throw new TimeSeriesException("Maximum number of candidates evaluated during matching, maxMatchCandidates, not found in properties file");
	}

	public static String getLocalDataCenter() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.LOCAL_DATA_CENTER) != null)
			return props.getProperty(TsrConstants.LOCAL_DATA_CENTER);
		throw new TimeSeriesException("Local data center not found in properties file");
	}
	
	public static String getClusterSeedsCoord() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLUSTER_SEEDS_COORD) != null)
			return props.getProperty(TsrConstants.CLUSTER_SEEDS_COORD);
		throw new TimeSeriesException("Coordination service seeds not found in properties file");
	}
	
	public static String getClusterSeedsCoordOverlay() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLUSTER_SEEDS_COORD_OVERLAY) != null)
			return props.getProperty(TsrConstants.CLUSTER_SEEDS_COORD_OVERLAY);
		throw new TimeSeriesException("Coordination service overlay seeds not found in properties file");
	}
	
	public static int getSessionTimeoutCoord() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SESSION_TIMEOUT_COORD) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.SESSION_TIMEOUT_COORD));
		throw new TimeSeriesException("Coordination service session timeout not found in properties file");
	}
	
	public static int getStatusTTL() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.STATUS_TTL) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.STATUS_TTL));
		throw new TimeSeriesException("Status TTL (expiration time) not found in properties file");
	}
	
	public static String getStoragePath() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.STORAGE_PATH) != null)
			return props.getProperty(TsrConstants.STORAGE_PATH);
		throw new TimeSeriesException("Storage path is not found in properties file");
	}
	
	public static String getStorageProvider() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.STORAGE_PROVIDER) != null)
			return props.getProperty(TsrConstants.STORAGE_PROVIDER);
		throw new TimeSeriesException("Storage provider is not found in properties file");
	}
	
	public static String getServiceLocation() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SERVICE_LOCATION) != null)
			return props.getProperty(TsrConstants.SERVICE_LOCATION);
		throw new TimeSeriesException("Service Location is not found in properties file");
	}
	
	public static int getMaxClaimsSync() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLAIMS_MAX_SYNC) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CLAIMS_MAX_SYNC));
		throw new TimeSeriesException("Maximum claims per sync worker not found in properties file");
	}
	
	public static int getMaxClaimsPattern() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLAIMS_MAX_PATTERN) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CLAIMS_MAX_PATTERN));
		throw new TimeSeriesException("Maximum claims per pattern index worker not found in properties file");
	}
	
	public static int getMaxClaimsSketch() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CLAIMS_MAX_SKETCH) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CLAIMS_MAX_SKETCH));
		throw new TimeSeriesException("Maximum claims per sketch index worker not found in properties file");
	}
	
	public static int getBatchSyncTaskCount() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.BATCH_SYNC_TASK_COUNT) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.BATCH_SYNC_TASK_COUNT));
		throw new TimeSeriesException("Batch task count for sync worker not found in properties file");
	}
	
	public static int getBatchIndexingTaskCount() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.BATCH_INDEXING_TASK_COUNT) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.BATCH_INDEXING_TASK_COUNT));
		throw new TimeSeriesException("Batch task count for indexing worker not found in properties file");
	}
	
	public static int getCEPEventExpirationWindow() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CEP_EVENT_EXPIRATION_WINDOW) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CEP_EVENT_EXPIRATION_WINDOW));
		throw new TimeSeriesException("CEP event expiration window not found in properties file");
	}
	
	public static int getCEPEventStorageBatchSize() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CEP_EVENT_STORAGE_BATCH_SIZE) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CEP_EVENT_STORAGE_BATCH_SIZE));
		throw new TimeSeriesException("CEP storage batch size not found in properties file");
	}
	
	public static int getCEPEventStorageInterval() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.CEP_EVENT_STORAGE_INTERVAL) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.CEP_EVENT_STORAGE_INTERVAL));
		throw new TimeSeriesException("CEP event storage interval not found in properties file");
	}
	
	public static int getSqlQueryLimit() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.SQL_QUERY_LIMIT) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.SQL_QUERY_LIMIT));
		throw new TimeSeriesException("SQL query limit not found in properties file");
	}
	
	public static int getDataHandlerThreads() throws TimeSeriesException {
		if (props != null && props.getProperty(TsrConstants.DATA_HANDLER_THREADS) != null)
			return Integer.parseInt(props.getProperty(TsrConstants.DATA_HANDLER_THREADS));
		throw new TimeSeriesException("Data handler thread count not found in properties file");
	}
	
	public static void main(String[] args) throws TimeSeriesException {
		printProperties();
	}
	
	public static void printProperties() throws TimeSeriesException {
		System.out.println(TsrConstants.CLUSTER_NAME + "=" + getClusterName());
		System.out.println(TsrConstants.NODE_LOGICAL_NUMBER + "=" + getNodeLogicalNumber());
		System.out.println(TsrConstants.INDEX_CHUNK_SIZE + "=" + getIndexChunkSize());
		System.out.println(TsrConstants.INDEX_NUM_CHUNKS + "=" + getIndexNumChunks());
		System.out.println(TsrConstants.SKETCH_CHUNK_SIZE + "=" + getIndexChunkSize());
		System.out.println(TsrConstants.SKETCH_NUM_CHUNKS + "=" + getIndexNumChunks());
		System.out.println(TsrConstants.HANDLER_SERIALIZER + "=" + getSerializerType());
		System.out.println(TsrConstants.OVERLAY_TIMER_INTERVAL + "=" + getOverlayTimerInterval());
		System.out.println(TsrConstants.OVERLAY_VDB_FILE + "=" + getOverlayVdbFile());
		System.out.println(TsrConstants.PING_TIMER_INTERVAL + "=" + getPingTimerInterval());
		System.out.println(TsrConstants.NODE_ADDRESS + "=" + getNodeAddress());
		System.out.println(TsrConstants.NODE_DATA_CENTER + "=" + getNodeDataCenter());
		System.out.println(TsrConstants.NODE_NAME + "=" + getNodeName());
		System.out.println(TsrConstants.IS_BOOTSTRAP_SERVER + "=" + isBoostrapNode());
		System.out.println(TsrConstants.IS_SEED_SERVER + "=" + isSeedNode());
		System.out.println(TsrConstants.INSTANCE_ID + "=" + getInstanceId());
		System.out.println(TsrConstants.SCRIPT_LOCATION + "=" + getScriptLocation());
		System.out.println(TsrConstants.SERVICE_LOCATION + "=" + getServiceLocation());
		System.out.println(TsrConstants.NODE_IS_REPLICA_OF + "=" + isReplicaOf());
		System.out.println(TsrConstants.NODE_DATA_STORAGE + "=" + getServerStorage());
		System.out.println(TsrConstants.NODE_SERVER_TYPE + "=" + getServerType());
		System.out.println(TsrConstants.MAX_MATCH_CANDIDATES + "=" + getMaxMatchCandidates());
		System.out.println(TsrConstants.LOCAL_DATA_CENTER + "=" + getLocalDataCenter());
		System.out.println(TsrConstants.CLUSTER_SEEDS_COORD + "=" + getClusterSeedsCoord());
		System.out.println(TsrConstants.CLUSTER_SEEDS_COORD_OVERLAY + "=" + getClusterSeedsCoordOverlay());
		System.out.println(TsrConstants.SESSION_TIMEOUT_COORD + "=" + getSessionTimeoutCoord());
		System.out.println(TsrConstants.STATUS_TTL + "=" + getStatusTTL());
		System.out.println(TsrConstants.STORAGE_PATH + "=" + getStoragePath());
		System.out.println(TsrConstants.STORAGE_PROVIDER + "=" + getStorageProvider());
		System.out.println(TsrConstants.SERVICE_LOCATION + "=" + getServiceLocation());
		System.out.println(TsrConstants.CLAIMS_MAX_SYNC + "=" + getMaxClaimsSync());
		System.out.println(TsrConstants.CLAIMS_MAX_PATTERN + "=" + getMaxClaimsPattern());
		System.out.println(TsrConstants.CLAIMS_MAX_SKETCH + "=" + getMaxClaimsSketch());
		System.out.println(TsrConstants.BATCH_SYNC_TASK_COUNT + "=" + getBatchSyncTaskCount());
		System.out.println(TsrConstants.BATCH_INDEXING_TASK_COUNT + "=" + getBatchIndexingTaskCount());
		System.out.println(TsrConstants.CEP_EVENT_EXPIRATION_WINDOW + "=" + getCEPEventExpirationWindow());
		System.out.println(TsrConstants.CEP_EVENT_STORAGE_BATCH_SIZE + "=" + getCEPEventStorageBatchSize());
		System.out.println(TsrConstants.CEP_EVENT_STORAGE_INTERVAL + "=" + getCEPEventStorageInterval());
		System.out.println(TsrConstants.SQL_QUERY_LIMIT + "=" + getSqlQueryLimit());
		System.out.println(TsrConstants.DATA_HANDLER_THREADS + "=" + getDataHandlerThreads());
	}
	
	private static void loadPropertiesFile() {
		
		String propFile = System.getenv(TsrConstants.TSR_PROPERTIES_FILE);	//variable "tsrPropFile" must point to fully qualified path

		if (propFile == null) {
			logger.debug("TSR properties file location not set in environment: " + TsrConstants.TSR_PROPERTIES_FILE);
			propFile = System.getProperty(TsrConstants.TSR_PROPERTIES_FILE);
		}

		if (propFile == null) {
			logger.debug("TSR properties file location not set in jvm properties: " + TsrConstants.TSR_PROPERTIES_FILE);
			propFile = "LocalNodeProperties.config";
		}

		if (!(new File(propFile)).exists()) {
			logger.error("TSR properties file not found anywhere, exiting: " + TsrConstants.TSR_PROPERTIES_FILE);
			System.exit(1);
		}

		FileInputStream in = null;
		 try
		 {
			 props = new Properties();
			 in = new FileInputStream(propFile);
			 props.load( in );
		 } catch (FileNotFoundException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
	     finally
	     {
	         try {
				in.close( );
			} catch (IOException e) {
				logger.error(e);
				e.printStackTrace();
			}
	     }
	}
}
