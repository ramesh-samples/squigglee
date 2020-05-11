// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.config;

//import org.joda.time.DateTimeZone;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;

public class TsrConstants {
	public static final long   TOKEN_SIZE = 1000000000;
	public static final String KEYSPACE_PREFIX = "ks_data";
	public static final String KEYSPACE_SEPARATOR = "_";
	public static final String CLUSTER_NAME = "clusterName";
	public static final String NODE_LOGICAL_NUMBER = "logicalNumber";
	public static final String NODE_IS_REPLICA_OF = "replicaOf";
	public static final String INDEX_CHUNK_SIZE = "indexChunkSize";
	public static final String INDEX_NUM_CHUNKS = "indexNumChunks";
	public static final String SKETCH_CHUNK_SIZE = "sketchChunkSize";
	public static final String SKETCH_NUM_CHUNKS = "sketchNumChunks";	
	public static final String OVERLAY_TIMER_INTERVAL = "overlayTimerInterval"; //in seconds 
	public static final String OVERLAY_VDB_FILE = "vdbFile"; // fully qualified path name
	public static final String PING_TIMER_INTERVAL = "pingTimerInterval"; //in seconds
	public static final String CONFIGURATION_SERVICE_INTERVAL = "configurationServiceInterval"; //in seconds
	public static final String HANDLER_CLASS = "handlerClass";
	public static final String HANDLER_SERIALIZER = "serializerType";
	public static final String HANDLER_SERIALIZER_AVRO = "AVRO";
	public static final String HANDLER_SERIALIZER_PB = "PB";
	public static final int    COLUMN_FAMILY_MAX_COLUMNS = 1000000000; // 1 billion and 1 columns
	public static final String NODE_ADDRESS = "addr";
	public static final String INSTANCE_ID = "id";
	public static final String NODE_DATA_CENTER = "dc";
	public static final String NODE_NAME = "name";
	public static final String NODE_DATA_STORAGE = "storage";
	public static final String NODE_SERVER_TYPE = "stype";
	public static final String IS_BOOTSTRAP_SERVER = "isBoot";
	public static final String IS_SEED_SERVER = "isSeed";
	public static final String SCRIPT_LOCATION = "scriptLocation";
	public static final String SERVICE_LOCATION = "serviceLocation";
	public static final String TSR_PROPERTIES_FILE = "tsrPropFile";
	public static final String ANSIBLE_EXE = "ansibleExe";
	public static final String MAX_MATCH_CANDIDATES = "maxMatchCandidates";
	public static final String LOCAL_DATA_CENTER = "localDataCenter";
	public static final String CLUSTER_SEEDS_COORD = "zkSeeds";
	public static final String CLUSTER_SEEDS_COORD_OVERLAY = "zkovSeeds";
	public static final String SESSION_TIMEOUT_COORD = "sessionTimeoutCoord";
	public static final String STATUS_TTL = "statusTTL";
	public static final String ROOT_PATH = "/SQUIGGLEE";
	public static final String STORAGE_PATH = "tsStoragePath";
	public static final String STORAGE_PROVIDER = "tsStorageProvider";
	public static final String CLAIMS_MAX_SYNC = "maxClaimsSync";
	public static final String CLAIMS_MAX_PATTERN = "maxClaimsPattern";
	public static final String CLAIMS_MAX_SKETCH = "maxClaimsSketch";
	public static final String BATCH_SYNC_TASK_COUNT = "batchSyncTaskCount";
	public static final String BATCH_INDEXING_TASK_COUNT = "batchIndexingTaskCount";
	public static final String CEP_EVENT_EXPIRATION_WINDOW = "cepEventExpiration";
	public static final String CEP_EVENT_STORAGE_BATCH_SIZE = "cepEventStorageBatchSize";
	public static final String CEP_EVENT_STORAGE_INTERVAL = "cepEventStorageInterval";
	public static final String SQL_QUERY_LIMIT = "sqlQueryLimit";
	public static final String DATA_HANDLER_THREADS = "dataHandlerThreads";
}
