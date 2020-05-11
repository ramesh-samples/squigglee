// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import com.squigglee.coord.storage.PatternHandlerMixin;
import com.squigglee.core.config.FileShard;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Match;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.sketch.LocalitySensitiveHasher;

public class IPatternHandlerImpl extends IIndexHandlerImpl implements IPatternHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.PatternIndexHandlerImpl");
	protected static PatternHandlerMixin phMixin = null;
	protected static FileShard indexShard;
	protected static Map<Long,FileShard> multiIndexShards = null;
	protected static Map<Long,Map<Integer,MappedByteBuffer>> indexBuffers = null;
	protected static Map<Long,Map<Integer,MappedByteBuffer>> multiIndexBuffers = null;
	protected static int nthreads = 16;
	protected static ExecutorService executorService = null;
	protected int nextId;
	
	@Override
	public void initialize() {
		super.initialize();
		//nthreads = 16;
		if (indexShard == null)
			indexShard = new FileShard(4, nthreads, ( ((long) Integer.MAX_VALUE) - ((long) Integer.MIN_VALUE) + 1L)*4L);
		if (indexBuffers == null)
			indexBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
		//if (multiIndexShard == null)
		//	multiIndexShard = new FileShard(40, 10, 100000000*40L);
		if (multiIndexShards == null)
			multiIndexShards = new HashMap<Long, FileShard>();
		if (multiIndexBuffers == null)
			multiIndexBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
		if (executorService == null)
			executorService = Executors.newFixedThreadPool(nthreads);
		if (phMixin == null)
			phMixin = new PatternHandlerMixin(this, this.deserializer, this.serializer);
	}
	
	@Override
	public List<Long> fetchCandidatePatterns(MasterData md, String idxTableName, List<Integer> hashes)
			throws TimeSeriesException {
		List<Long> candidates = new ArrayList<Long>();
		for (Integer queryhash : hashes) {
			List<Long> vals = new ArrayList<Long>();
			try {
				vals = readIndex(md, idxTableName, queryhash);
			} catch (IOException e) {
				logger.error("Found error fetching candidate patterns for index = " + idxTableName + " and masterdata = " + md,e);
			}
			for (Long val : vals)
				if (val != -1 && !candidates.contains((long) val))
					candidates.add((long) val);
		}
		return candidates;
	}

	@Override
	public Map<Long, SortedMap<Long, Object>> fetchCandidateTimeSeries(MasterData md, List<Long> candidates, int size)
			throws TimeSeriesException {
		//TODO proxy this call 
		SortedMap<Long,SortedMap<Long, Object>> output = new TreeMap<Long,SortedMap<Long, Object>>();
		for (long c : candidates) {
			SortedMap<Long, Object> fetchedTs = fetchTimeSeriesLimit(md,c, (long) TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, size,  false);
			output.put(c,fetchedTs);
		}
		return output;
	}

	@Override
	public void processMatches(Matches request) {
		try {
			List<MasterData> mdList = new ArrayList<MasterData>();
			for (TimeSeriesConfig tc : request.getRequestDomain()) {
				//System.out.println(tc);
				if (request.isDataLocal() && tc.getLogicalNode() == localln) 
					mdList.addAll(getMasterData(tc.getCluster(), replicaOf, tc.getGuid(), 
						tc.getStartDate().getMillis(),tc.getEndDate().getMillis()));
			}
			
			//System.out.println("Master data list for matches = " + mdList);
			
			SortedMap<Double,List<Match>> matchedList = new TreeMap<Double,List<Match>>();
			//TODO patterns can also be stored using the pguid rather than explicitly provided via input
			int patternSize = request.getPattern().getVals().size();
			double[] norm_pattern = LocalitySensitiveHasher.normalize(request.getPattern().getVals().toArray());
			for (MasterData md : mdList) {
				matchedList.clear();
				LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);	//does not matter for querying what the replicaCount & replicaNumber values are
				if (md.getIndexes() == null || !md.getIndexes().toLowerCase().contains("ptrn"))
					continue;
				String ptrnIndex = null;  
				String[] indices = md.getIndexes().split(";");
				for (String idx : indices) {
					if (idx.toLowerCase().contains("ptrn")) {
						ptrnIndex = idx;
						break;		// currently supporting only one pattern index at a time
					}
				}
				if (ptrnIndex == null)
					throw new TimeSeriesException("No pattern indexes are available for node " + md.getLn() + " and parameter " + md.getGuid());
				lsh.loadSerializedIndex(md.getCluster(), md.getId(), ptrnIndex, this);	 
				
				List<Long> candidates = fetchCandidatePatterns(md, lsh.getIndexTableName(), lsh.getHashes(request.getPattern().getVals()));
				logger.debug("Found " + candidates + " potential match candidates for master data " + md);
				System.out.println("Found " + candidates + " potential match candidates for master data " + md);
				if (candidates.size() > LocalNodeProperties.getMaxMatchCandidates()) {
					logger.debug("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
					System.out.println("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
					candidates = candidates.subList(0, (LocalNodeProperties.getMaxMatchCandidates() - 1));
				}
				
				Map<Long, SortedMap<Long, Object>> candidateTimeSeries = fetchCandidateTimeSeries(md, candidates, patternSize);
				for (long c : candidates) {
					double[] ts_norm = LocalitySensitiveHasher.normalize(candidateTimeSeries.get(c).values());
					double dist = LocalitySensitiveHasher.getDistance(ts_norm,norm_pattern);
					if (dist <= request.getRadius()) {
						Match match = new Match(md.getCluster(), md.getLn(), md.getGuid(), md.getDatatype(), md.getFreq().toString()
								, md.getStartts(), candidateTimeSeries.get(c), dist);
						if (!matchedList.containsKey(dist))
							matchedList.put(dist, new ArrayList<Match>());
						matchedList.get(dist).add(match);
					}
				}
				System.out.println(matchedList.size() + " matches found within requested radius found for master data " + md);
				request.addMatches(matchedList);
			}
		} catch (TimeSeriesException tse) {
			logger.error("Found error processing matches request " + request, tse);
		}
	}
	
	
	public void updatePatternIndex(MasterData md, String idxTableName, long startKey, long endKey,
			Map<Integer, List<Long>> lookup, int projHashCount, int projHashNumber) throws TimeSeriesException {

		if (!getConfigurationStatus(md.getCluster(), md.getId(),idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			return;
		}
		writeIndex(md, idxTableName, lookup);		
	}
	
	@Override
	public boolean storePattern(String cluster, String pguid, List<String> pattern) throws TimeSeriesException {
		return phMixin.storePattern(cluster, pguid, pattern);
	}

	@Override
	public List<String> fetchPattern(String cluster, String pguid) throws TimeSeriesException {
		return phMixin.fetchPattern(cluster, pguid);
	}
	
	@Override
	public List<String> fetchCapturedPatterns(String cluster) throws TimeSeriesException {
		return phMixin.fetchCapturedPatterns(cluster);
	}
		
	public View getPatternView(View matchView) {
		return phMixin.getPatternView(matchView);
	}

	protected MappedByteBuffer getIndexBuffer(String indexName, long id, int shardNum) throws IOException {

		if (!indexBuffers.containsKey(id)) {
			indexBuffers.put(id, new TreeMap<Integer,MappedByteBuffer>());
		}
		if (!indexBuffers.get(id).containsKey(shardNum)) {
			File f = new File(storagePath + "/" + indexName + "_" + id);
			if (!f.exists() || f.length() <= 17179869184L) {
				synchronized(this) {
					preAllocateIndexFile(f);
				}
			}
			long shardSize = indexShard.getShardSize();
			long shardStart = shardSize*shardNum;
			MappedByteBuffer mbb = getMappedBuffer(f, shardStart, shardSize);
			indexBuffers.get(id).put(shardNum, mbb);
		}
		return indexBuffers.get(id).get(shardNum);
		
	}
	
	protected MappedByteBuffer getMultiIndexBuffer(String indexName, long id, int shardNum) throws IOException {

		if (!multiIndexBuffers.containsKey(id)) {
			multiIndexBuffers.put(id, new TreeMap<Integer,MappedByteBuffer>());
		}
		if (!multiIndexBuffers.get(id).containsKey(shardNum)) {
			File f = new File(storagePath + "/" + indexName + "_" + id + "_multi");
			if (!f.exists() || f.length() < 10000000000L) {
				synchronized(this) {
					preAllocateMultiIndexFile(f);
				}
			}
			long shardSize = getMultiIndexShard(indexName,id).getShardSize();
			long shardStart = shardSize*shardNum;
			MappedByteBuffer mbb = getMappedBuffer(f, shardStart, shardSize);
			multiIndexBuffers.get(id).put(shardNum, mbb);
		}
		return multiIndexBuffers.get(id).get(shardNum);
	}
	
	protected FileShard getMultiIndexShard(String indexName, long id) throws IOException {
		if (!multiIndexShards.containsKey(id)) {
			File f = new File(storagePath + "/" + indexName + "_" + id + "_multi");
			if (!f.exists() || f.length() < 10000000000L) {
				synchronized(this) {
					preAllocateMultiIndexFile(f);
				}
			}
			multiIndexShards.put(id, new FileShard(40, 10, f.length()));
		}
		return multiIndexShards.get(id);
	}
	
	protected Runnable getTask(int tid, String indexName, long id, Map<Integer, List<Long>> indexedValues, CountDownLatch cdLatch) {
		return new Runnable() {
			int tid = 0;
			String indexName = null;
			long id = 0L;
			Map<Integer, List<Long>> indexedValues = null;
			private CountDownLatch cdLatch = null;
			@Override
			public void run() {
				//System.out.println("Launching storage thread with id = " + tid + " for index " + indexName + " and id = " + id);
				try {
					//int stored = 0;
					//DateTime start = DateTime.now();
					MappedByteBuffer map = null;
					//int processed = 0;
					for (int hash : indexedValues.keySet()) {
						List<Long> vals = indexedValues.get(hash);
						if (vals.size() == 0)
							continue;						
						//int index = MurmurHash.hash32(hash);
						//long actualRow = (((long) index) - ((long) Integer.MIN_VALUE));
						long actualRow = (((long) hash) - ((long) Integer.MIN_VALUE));
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
									int currentVal = map.getInt((row*indexShard.getDataSize()));
									if (vals.size() == 1 && currentVal == vals.get(0))		//skip the redundant update 
										break;
									else if (currentVal == -1 && vals.size() == 1) {
										map.putInt((int) (row*indexShard.getDataSize()), vals.get(0).intValue());
										break;
									} else {
										int nextId = updateMultiFile(vals, currentVal, indexName, id);
										if (nextId != -1)
											map.putInt((int) (row*indexShard.getDataSize()), (nextId + TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
										break;
									}
								} catch (Exception ex) {	// sometimes throws an exception
									logger.error("Found error putting mapped value for row " + row + " and thread = " + shardNum, ex);
									System.out.println("Resetting the map after error for row " + row + " and thread = " + shardNum + " and try = " + retry);
									try {
										Thread.sleep(2000);
									} catch (InterruptedException e) {
										logger.error("Error while sleeping");
									}
								}
							}
							//stored++;
						}
						//processed++;
					}
					if (map != null)
						map.force();
					//DateTime end = DateTime.now();
					//System.out.println("Completing index storage for thread = " + tid + " for index " + indexName + " and id = " + id 
					//		+ " with stored count = " + stored + " out of processed count = " + processed + " in time = " + (new Interval(start, end)).toDurationMillis() + " millis");
				} finally {
					cdLatch.countDown();
				}
			}
			
			public Runnable initialize(int threadid, String indexname, long id, Map<Integer, List<Long>> indexedValues, CountDownLatch cdLatch) {
				this.tid = threadid;
				this.indexName = indexname;
				this.id = id;
				this.indexedValues = indexedValues;
				this.cdLatch = cdLatch;
				return this;
			}
		}.initialize(tid, indexName, id, indexedValues, cdLatch);
	}
	
	protected void writeIndex(MasterData md, String indexName, Map<Integer, List<Long>> indexedValues) {
		CountDownLatch cdLatch = new CountDownLatch(nthreads);
		//System.out.println("Total indexed values = " + indexedValues.keySet().size());
		nextId = getNextId(md.getCluster(), indexName, md.getId());

		//System.out.println("Starting nextId = " + nextId);
		for (int i =0; i < nthreads ; i++)
			executorService.execute(getTask(i, indexName, md.getId(), indexedValues, cdLatch));
		try {
			cdLatch.await();
		} catch (InterruptedException e) {
			logger.error("Error waiting for all threads to complete storing the pattern index " + indexName + " for masterdata " + md);
		}
		//System.out.println("Update ending nextId = " + nextId);
		setNextId(md.getCluster(), indexName, md.getId(), nextId);
	}
	
	protected List<Long> readIndex(MasterData md, String indexName, int hash) throws IOException {
		//int index = MurmurHash.hash32(hashes);
		//long actualRow = (((long) index) - ((long) Integer.MIN_VALUE));
		
		long actualRow = (((long) hash) - ((long) Integer.MIN_VALUE));
		long currentPosition = actualRow*indexShard.getDataSize();
		int row = (int) (currentPosition - indexShard.getShardStart(actualRow))/indexShard.getDataSize();
		int shardNum = indexShard.getShardNumber(actualRow);
		MappedByteBuffer map = getIndexBuffer(indexName, md.getId(), shardNum);
		int val = map.getInt(row*indexShard.getDataSize());
		List<Long> vals = new ArrayList<Long>();
		vals.add((long) val);
		if (val >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS)
			vals = getMultiVals(indexName, md.getId(), (val - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS));
		
		return vals;
	}
	
	private List<Long> getMultiVals(String indexName, long id, int rowid) throws IOException {
		List<Long> vals = new ArrayList<Long>();      
		long currentPosition = rowid*getMultiIndexShard(indexName,id).getDataSize();
		int shardRow = (int) (currentPosition - getMultiIndexShard(indexName,id).getShardStart(rowid))/getMultiIndexShard(indexName,id).getDataSize();
		MappedByteBuffer map = getMultiIndexBuffer(indexName, id, getMultiIndexShard(indexName,id).getShardNumber(rowid));
		int counter = 0;
		for (int i=0; i<10; i++) {
			long v = (long) map.getInt(shardRow*getMultiIndexShard(indexName,id).getDataSize() + counter*4);
			counter++;
			if (v != -1 && !vals.contains(v))
				vals.add(v);
		}
		return vals;
	}

	protected int updateMultiFile(List<Long> vals, int currentVal, String indexName, long id) throws IOException {
		MappedByteBuffer map = null;
		if (currentVal != -1 && currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS && !vals.contains(currentVal))
			vals.add(0, (long) currentVal);
		if (currentVal < TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
			int nextId = getNextId();
			
			//System.out.print("Next id = " + nextId);
			long currentPosition = nextId*getMultiIndexShard(indexName,id).getDataSize();
			int shardRow = (int) (currentPosition - getMultiIndexShard(indexName,id).getShardStart(nextId))/getMultiIndexShard(indexName,id).getDataSize();
			map = getMultiIndexBuffer(indexName, id, getMultiIndexShard(indexName,id).getShardNumber(nextId));
			int counter = 0;
			for (Long v : vals) {
				map.putInt(shardRow*getMultiIndexShard(indexName,id).getDataSize() + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
			return nextId;
		} else if (currentVal >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS) {
			int dsize = getMultiIndexShard(indexName,id).getDataSize();
			int multiId = currentVal - TsrConstants.COLUMN_FAMILY_MAX_COLUMNS;
			long currentPosition = multiId*1L*dsize;
			int shardRow = (int) (currentPosition - getMultiIndexShard(indexName,id).getShardStart(multiId))/dsize;
			map = getMultiIndexBuffer(indexName, id, getMultiIndexShard(indexName,id).getShardNumber(multiId));
			boolean changed = false;
			List<Long> storedValues = new ArrayList<Long>();
			for (int i=0; i<10; i++) {
				long v = (long) map.getInt(shardRow*dsize);
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
				map.putInt(shardRow*dsize + counter*4, v.intValue());
				counter++;
				if (counter == 10)
					break;
			}
			return multiId;
		}
		return -1;
	}
	
	protected synchronized int getNextId() {
		return nextId++;
	}

	protected int getNextId(String cluster, String indexName, long id) {
		try {
			return phMixin.getNextMultiIndexId(cluster, id, indexName);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching next id for pattern multi index for cluster = " + cluster + " and id = " + id + " and index = " + indexName,e);
		}
		return 0;
	}
	
	protected void setNextId(String cluster, String indexName, long id, int currentId) {
		try {
			phMixin.setNextMultiIndexId(cluster, id, indexName, currentId);
		} catch (TimeSeriesException e) {
			logger.error("Found error updating next id for pattern multi index for cluster = " + cluster + " and id = " + id + " and index = " + indexName,e);
		}
	}
}
