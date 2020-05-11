package com.squigglee.core.utility;

import ie.ucd.murmur.MurmurHash;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.config.FileShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;

public class MappedIndexHandler extends MappedHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.MappedDataHandler");
	private static String storagePath = null;
	private static FileShard indexShard;
	private static FileShard indexMultiShard;
	private static Map<Long,Map<Integer,MappedByteBuffer>> indexBuffers = null;
	private static Map<Long,Map<Integer,MappedByteBuffer>> indexMultiBuffers = null;
	protected static int nthreads = 0;
	protected static ExecutorService executorService = null;
	
	static {
		try {
			nthreads = 16;
			storagePath = LocalNodeProperties.getStoragePath();
			//all patterns hashed to an int of 4 bytes == 17,179,869,184 bytes of storage; one shard per thread
			long delta = ( ((long) Integer.MAX_VALUE) - ((long) Integer.MIN_VALUE) + 1L);
			long totalSize = delta*4;
			indexShard = new FileShard(4, nthreads, totalSize);
			//roughly 5 % with multiple values hashed to same index so around 9.6GB of additional storage 
			//indexMultiShard =  new FileShard(40, 20, (240000000L)*40L);

			indexBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
			//indexMultiBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
			
			executorService = Executors.newFixedThreadPool(nthreads);
		} catch (TimeSeriesException e) {
			logger.error("Failed to execute static initializer for MappedDataHandler", e);
		}
	}
	
	public static void writeIndex(MasterData md, String indexName, Map<String, List<Long>> indexedValues) {
		for (int i =0; i < nthreads ; i++)
			executorService.execute(getTask(i, indexName, md.getId(), indexedValues));
	}
	
	public static List<Long> readIndex(MasterData md, String indexName, String hashes) {
		List<Long> vals = new ArrayList<Long>();
		int index = MurmurHash.hash32(hashes);
		long actualRow = (((long) index) - ((long) Integer.MIN_VALUE));
		long currentPosition = actualRow*indexShard.getDataSize();
		int row = (int) (currentPosition - indexShard.getShardStart(actualRow))/indexShard.getDataSize();
		int shardNum = indexShard.getShardNumber(actualRow);
		MappedByteBuffer map = getIndexBuffer(indexName, md.getId(), shardNum);
		int val = map.getInt(row*indexShard.getDataSize());
		
		if (val >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
			int multiIndexRowId = (val - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS);
			long currentMultiIndexPosition = multiIndexRowId*indexMultiShard.getDataSize();
			int multiIndexShardNum = indexMultiShard.getShardNumber(multiIndexRowId);
			int multiRow = (int) (currentMultiIndexPosition - indexMultiShard.getShardStart(currentMultiIndexPosition))/indexMultiShard.getDataSize();
			MappedByteBuffer multIndexMap = getIndexMultiBuffer(indexName, md.getId(), multiIndexShardNum);
			
			for (int i=0; i<10; i++) {
				long v = (long) multIndexMap.getInt(multiRow*indexMultiShard.getDataSize() + i*4);
				if (v != -1 && !vals.contains(v))
					vals.add(v);
			}
		} else 
			vals.add((long) val);
		return vals;
	}
	
	private static MappedByteBuffer getIndexBuffer(String indexName, long id, int shardNum) {

			if (!indexBuffers.containsKey(id)) {
				indexBuffers.put(id, new TreeMap<Integer,MappedByteBuffer>());
			}
			if (!indexBuffers.get(id).containsKey(shardNum)) {
				File f = new File(storagePath + "/" + indexName + "_" + id);
				long shardSize = indexShard.getShardSize();
				long shardStart = shardSize*shardNum;
				MappedByteBuffer mbb = getMappedBuffer(f, shardStart, shardSize);
				//if (mbb != null)
					indexBuffers.get(id).put(shardNum, mbb);
				//else {
				//	System.out.println("Buffer is null for indexName = " + indexName  + " and id = " + id + " and shard number = " + shardNum);
				//}
			}
			return indexBuffers.get(id).get(shardNum);
	}
	
	private static MappedByteBuffer getIndexMultiBuffer(String indexName, long id, int shardNum) {

		if (!indexMultiBuffers.containsKey(id)) {
			indexMultiBuffers.put(id, new TreeMap<Integer,MappedByteBuffer>());
		}
		if (!indexMultiBuffers.get(id).containsKey(shardNum)) {
			File f = new File(storagePath + "/" + indexName + "_" + id + "_multi");
			long shardSize = indexMultiShard.getShardSize();
			long shardStart = shardSize*shardNum;
			indexMultiBuffers.get(id).put(shardNum, getMappedBuffer(f, shardStart, shardSize));
		}
		return indexMultiBuffers.get(id).get(shardNum);
}
	
	protected static Runnable getTask(int tid, String indexName, long id, Map<String, List<Long>> indexedValues) {
		return new Runnable() {
			int tid = 0;
			String indexName = null;
			long id = 0L;
			Map<String, List<Long>> indexedValues = null;
			
			@Override
			public void run() {
				System.out.println("Launching storage thread with id = " + tid + " for index " + indexName + " and id = " + id);
				int stored = 0;
				DateTime start = DateTime.now();
				MappedByteBuffer map = null;
				//int nextId = getNextId(0, indexName, id);
				for (String hash : indexedValues.keySet()) {
					List<Long> vals = indexedValues.get(hash);
					if (vals.size() == 0)
						continue;						
					int index = MurmurHash.hash32(hash);
					long actualRow = (((long) index) - ((long) Integer.MIN_VALUE));
					int shardNum = indexShard.getShardNumber(actualRow);
					if (shardNum == this.tid) {
						
						long currentPosition = actualRow*indexShard.getDataSize();
						int row = (int) (currentPosition - indexShard.getShardStart(actualRow))/indexShard.getDataSize();
						//int currentVal = -1;
						
						int maxRetry = 10;
						int retry = 0;
						while (retry <= maxRetry) {
							try {
								retry++;
								if (retry > maxRetry) {
									System.out.println("Aborting thread after failing to get map after " + maxRetry + " tries for thread = " + tid);
									return;				
								}
								map = getIndexBuffer(indexName, id, shardNum);
								map.putInt((int) (row*indexShard.getDataSize()), vals.get(0).intValue());
								break;
								//currentVal = map.getInt(row*indexShard.getDataSize());
							} catch (Exception ex) {	// sometimes throws an exception
								logger.error("Found error putting mapped value for row " + row + " and thread = " + shardNum, ex);
							//	if (indexBuffers!= null && indexBuffers.get(id) != null && indexBuffers.get(id).get(shardNum) != null)
							//		indexBuffers.get(id).get(shardNum).clear();
								System.out.println("Resetting the map after error for row " + row + " and thread = " + shardNum + " and try = " + retry);
								//map = getIndexBuffer(indexName, id, shardNum);
								//map.putInt((int) (row*indexShard.getDataSize()), vals.get(0).intValue());
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									 //TODO Auto-generated catch block
									 e.printStackTrace();
								}
								//map = getIndexBuffer(indexName, id, shardNum);
								//currentVal = map.getInt(row*indexShard.getDataSize());	//bubble it up the stack
							}
						}
						
						//if (vals.size() == 1 && currentVal == vals.get(0))		//skip the redundant update 
						//	continue;
						//if (currentVal == -1 && vals.size() == 1) {
							 
						//} else {
						//	nextId = updateMultiFile(nextId, vals, currentVal, indexName, id);
						//	map.putInt((int) (row*indexShard.getDataSize()), (nextId + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
						//}
						stored++;
						//if (stored % 100 == 0)
						//	map.force();
					}
				}
				if (map != null)
					map.force();
				DateTime end = DateTime.now();
				System.out.println("Completing index storage for thread = " + tid + " for index " + indexName + " and id = " + id 
						+ " with stored count = " + stored + " in time = " + (new Interval(start, end)).toDurationMillis() + " millis");
			}
			
			public Runnable initialize(int threadid, String indexname, long id, Map<String, List<Long>> indexedValues) {
				this.tid = threadid;
				this.indexName = indexname;
				this.id = id;
				this.indexedValues = indexedValues;
				return this;
			}
			
		}.initialize(tid, indexName, id, indexedValues);
	}
	
	/*
	private static synchronized int updateMultiFile(int nextId, List<Long> vals, int currentVal, String indexName, long id) {
		nextId = 0;
		MappedByteBuffer map = null;
		if (currentVal != -1 && currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS && !vals.contains(currentVal))
			vals.add(0, (long) currentVal);
		if (currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) { 
			//map = fc.map(MapMode.READ_WRITE, nextId*40, 40);	//each row is 10 ints
			nextId = getNextId(nextId, indexName, id);
			long currentPosition = nextId*indexMultiShard.getDataSize();
			int shardNum = indexMultiShard.getShardNumber(nextId);
			map = getIndexMultiBuffer(indexName, id, shardNum);
			int row = (int) (currentPosition - indexMultiShard.getShardStart(nextId))/indexMultiShard.getDataSize();
			int counter = 0;
			for (Long v : vals) {
				map.putInt((int) (row*indexMultiShard.getDataSize()) + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
		} else if (currentVal >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
			// values in main index table > 1,000,000,000 represent a pointer to the multi-table
			int multiId = currentVal - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
			long currentPosition = multiId*indexMultiShard.getDataSize();
			int shardNum = indexMultiShard.getShardNumber(multiId);
			map = getIndexMultiBuffer(indexName, id, shardNum);
			int row = (int) (currentPosition - indexMultiShard.getShardStart(multiId))/indexMultiShard.getDataSize();
			boolean changed = false;
			List<Long> storedValues = new ArrayList<Long>();
			for (int i=0; i<10; i++) {
				long v = (long) map.getInt((int) (row*indexMultiShard.getDataSize()) + i*4);
				if (v != -1)
					storedValues.add(v);
			}
			if (storedValues.size() == vals.size()) {
				for (Long v : storedValues)
					if (!vals.contains(v))
						changed = true;
			}
			if (!changed)
				return multiId;
			for (Long sv : storedValues) {
				if (sv != -1 && !vals.contains(sv))
					vals.add((long) sv);
			}
			int counter = 0;
			for (Long v : vals) {
				map.putInt((int) (row*indexMultiShard.getDataSize()) + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
			return currentVal;
		}
		if (map != null)
			map.force();
		return nextId;
	}
	
	private synchronized static int getNextId(int startRow, String indexName, long id) {
		MappedByteBuffer map = null;
		int nextId = 0;
		int shardNum = indexMultiShard.getShardNumber(startRow);
		boolean found = false;
		int rowCount = (int) (indexMultiShard.getShardSize()/indexMultiShard.getDataSize());
		for (int k=shardNum; k<indexMultiShard.getShardCount(); k++) {
			//long currentPosition = startRow*indexMultiShard.getDataSize();
			map = getIndexMultiBuffer(indexName, id, k);
			for (int i = 0; i< rowCount; i++) {
				for (int j=0; j<10; j++) {
					int val = map.getInt((int) (i*indexMultiShard.getDataSize()) + j*4);
					if (j == 0 && val == -1)
						found = true;
					if (found) {
						nextId = (rowCount*k + i);
						break;
					}
				}
				if (found)
					break;
			}
			if (found)
				break;
		}
		if (!found) {
			int max = (int) (indexMultiShard.getTotalSize() / indexMultiShard.getDataSize());
			System.out.println("Could not find the next id, using the maximum " + max);
			nextId = max;
		}
		//System.out.println("Current nextId = " + nextId);
		return nextId;
	}
	*/
}
