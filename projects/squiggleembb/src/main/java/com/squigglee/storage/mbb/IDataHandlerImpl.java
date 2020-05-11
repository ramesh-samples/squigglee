// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.storage.DataHandlerMixin;
import com.squigglee.core.config.FileShard;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.SyncTask;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.ITimeSeriesSerializer;
import com.squigglee.core.utility.DataUtility;

public class IDataHandlerImpl extends IMasterDataHandlerImpl implements IDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.IDataHandlerImpl");
	protected static DataHandlerMixin dhMixin = null;
	protected static Map<Long,Map<Integer,MappedByteBuffer>> dataBuffers = null;
	protected static Map<Long,FileShard> dataShards = null;
	
	@Override
	public void initialize() {
		super.initialize();
		if (dhMixin == null)
			dhMixin = new DataHandlerMixin(this.deserializer, this.serializer);
		if (dataShards == null)
			dataShards = new HashMap<Long, FileShard>();
		if (dataBuffers == null)
			dataBuffers = new HashMap<Long,Map<Integer,MappedByteBuffer>>();
	}
	
	@Override
	public boolean insertBulkData(byte[] bulkData) throws TimeSeriesException {
		if (bulkData == null || bulkData.length == 0)
			return false;
		deserializer.setRawData(bulkData);
		int[] insertCounts = new int[deserializer.getBlockCount()];
		for (int i = 0; i< insertCounts.length; i++) {
			int dataCount = 0;
			try {
				long startts = deserializer.getStartts(i);
				MasterData md = getMasterData(deserializer.getCluster(i), deserializer.getLn(i),deserializer.getGuid(i),startts);				
				if (md == null)
					throw new TimeSeriesException("No master data found for requested insert for cluster = " + deserializer.getCluster(i) 
							+ " ln = " + deserializer.getLn(i) + " and guid = " + deserializer.getGuid(i) + " and startts = " + startts);
				long initialOffset = 0;
				if (startts > md.getStartts())
					initialOffset = TimeSeriesShard.getOffset(md.getFreq(), startts);
				dataCount = deserializer.getDataCount(i);
				deleteData(md, (initialOffset + deserializer.getOffset(i,0)), (initialOffset + deserializer.getOffset(i,(dataCount - 1))));	
				postSyncJob(md.getCluster(), md.getId(), (initialOffset + deserializer.getOffset(i,0))
						, (initialOffset + deserializer.getOffset(i,(dataCount - 1))), CommandType.INSERT, bulkData);
				
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				System.out.println("Error inserting data");
				ex.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	public void postSyncJob(String cluster, long id, long start, long end, CommandType operation, byte[] data) throws TimeSeriesException {
		dhMixin.postSyncJob(cluster, id, start, end, operation, data);
	}
	
	@Override
	public boolean syncData(SyncTask syncTask) throws TimeSeriesException {
		if (syncTask.getDataOperation().equals(CommandType.INSERT))
			return syncInsertData(syncTask);
		else if (syncTask.getDataOperation().equals(CommandType.UPDATE)) {
			return syncInsertData(syncTask);
		} else if (syncTask.getDataOperation().equals(CommandType.DELETE)) {
			return syncDeleteData(syncTask);
		}
		return false;
	}
	
	private boolean syncInsertData(SyncTask syncTask) throws TimeSeriesException {
		boolean result = false;
		SortedMap<Long, Object> insertData = new TreeMap<Long,Object>();		
		deserializer.setRawData(syncTask.getData());
		int[] insertCounts = new int[deserializer.getBlockCount()];
		for (int i = 0; i< insertCounts.length; i++) {
			int dataCount = 0;
			try {
				long startts = deserializer.getStartts(i);
				MasterData masterData = getMasterData(deserializer.getCluster(i), deserializer.getLn(i),deserializer.getGuid(i),startts);
				if (masterData == null)
					throw new TimeSeriesException("No master data found for requested insert");
				Type dataType = DynamicTypeTranslator.getSchemaType(masterData.getDatatype());				
				deserializer.resetSchema(dataType);
				deserializer.setRawData(syncTask.getData());
				
				long initialOffset = 0;
				if (startts > masterData.getStartts())
					initialOffset = TimeSeriesShard.getOffset(masterData.getFreq(), startts);
				dataCount = deserializer.getDataCount(i);

				for (int j = 0; j< dataCount; j++) 	{
					insertData.put((initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j));
				}
				
				//raw inserts
				writeBufferData(masterData, insertData);
				long datamin = (initialOffset + deserializer.getOffset(i,0));
				long datamax = (initialOffset + deserializer.getOffset(i,(dataCount - 1)));
				updateDataStatus(masterData,datamin, datamax);
				
				//rollup inserts
				for (Frequency rollupFreq : TimeSeriesShard.getRollupFrequencies(masterData.getFreq())) {
					List<TimeSeriesConfig> rollupConfig = getMasterData(deserializer.getCluster(i), deserializer.getLn(i),
							deserializer.getGuid(i) + "_" + rollupFreq);
					if (rollupConfig == null || rollupConfig.isEmpty())
						continue;
					Map<Long, SortedMap<Long, Object>> rollup = TimeSeriesShard.getRollUp(insertData, masterData, rollupFreq);
					for (Long rollupStartts : rollup.keySet()) {
						MasterData rollupMasterData = getMasterData(deserializer.getCluster(i), deserializer.getLn(i),
								deserializer.getGuid(i) + "_" + rollupFreq, rollupStartts);
						if (rollupMasterData == null)
							continue;
						writeBufferData(rollupMasterData, rollup.get(rollupStartts));
						long rolledupdatamin = rollup.get(rollupStartts).firstKey();
						long rolledupdatamax = rollup.get(rollupStartts).lastKey();
						updateDataStatus(rollupMasterData,rolledupdatamin, rolledupdatamax);
					}
				}
				
				result = true;
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				throw new TimeSeriesException(ex.getMessage() + " for cluster = " + deserializer.getCluster(i) + " guid = " + deserializer.getGuid(i) 
						+ " and startts = " + new DateTime(deserializer.getStartts(i),DateTimeZone.UTC));
			}
		}
		return result;
	}
	
	private boolean syncDeleteData(SyncTask syncTask) throws TimeSeriesException {
		MasterData md = getMasterData(syncTask.getCluster(), syncTask.getId());
		if (md == null)
			return false;
		deleteBufferData(md, syncTask.getStartoffset(), syncTask.getEndoffset());
		
		//rollup deletes
		for (Frequency rollupFreq : TimeSeriesShard.getRollupFrequencies(md.getFreq())) {
			List<MasterData> rollupMasterDataList = getMasterData(md.getCluster(), md.getLn(),
				md.getGuid() + "_" + rollupFreq, TimeSeriesShard.advance(md.getFreq(), md.getStartts(), (int) syncTask.getStartoffset()).getMillis(),
				TimeSeriesShard.advance(md.getFreq(), md.getStartts(), (int) syncTask.getEndoffset()).getMillis());
			if (rollupMasterDataList == null || rollupMasterDataList.isEmpty())
				continue;
			Map<Long, Set<Long>> rollup = TimeSeriesShard.getRollUp(syncTask.getStartoffset(), syncTask.getEndoffset(), md, rollupFreq);
			for (Long rollupStartts : rollup.keySet()) {
				MasterData rollupMasterData = getMasterData(md.getCluster(), md.getLn(),
						md.getGuid() + "_" + rollupFreq, rollupStartts);
				if (rollupMasterData == null)
					continue;
				if (rollup.get(rollupStartts).isEmpty())
					continue;
					//throw new TimeSeriesException("Rollup is empty for sync task " + syncTask + " for rollup master data " + rollupMasterData);
				deleteBufferData(rollupMasterData, Long.parseLong(rollup.get(rollupStartts).toArray()[0].toString()), 
						Long.parseLong(rollup.get(rollupStartts).toArray()[rollup.get(rollupStartts).toArray().length-1].toString()));
			}
		}
		return true;
	}

	@Override
	public void updataSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		dhMixin.updataSyncStatus(syncTask);
	}
	
	@Override
	public void postIndexingJobs(SyncTask syncTask) throws TimeSeriesException {
		dhMixin.postIndexingJobs(syncTask);
	}
	
	@Override
	public boolean updateBulkData(byte[] bulkData) throws TimeSeriesException {
		return insertBulkData(bulkData);
	}
	
	@Override
	public boolean deleteData(MasterData md, long start, long end) throws TimeSeriesException {
		boolean result = true;
		byte[] priorData = (byte[]) readBlockData(md, start, end)[2];	
		// nothing to delete or re-index, the single array element is the default block count
		if (priorData == null || priorData.length <= 1)		 
			return false; 
		postSyncJob(md.getCluster(), md.getId(), start, end, CommandType.DELETE, priorData);
		return result;
	}
	
	@Override
	public boolean deleteData(String cluster, int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException {
		for (MasterData md : getMasterData(cluster, ln, guid, begints, endts)) {
			if (TimeSeriesShard.ignoreOffsets(md.getFreq())) {
				beginoffset = 0;
				endoffset = 0;
			}
			DateTime rowEndTs = TimeSeriesShard.getMaxEndDate(md.getFreq(), md.getStartts());
			long initialOffset = beginoffset;
			if (begints > md.getStartts())
				initialOffset = TimeSeriesShard.getOffset(md.getFreq(), begints);
			long finalOffset;
			if ((endts) < rowEndTs.getMillis())
				finalOffset = TimeSeriesShard.getOffset(md.getFreq(), endts);
			else
				finalOffset = TimeSeriesShard.getOffset(md.getFreq(), rowEndTs);
			
			byte[] priorData = (byte[]) readBlockData(cluster, ln, guid, begints, beginoffset, endts, endoffset)[2];
			// nothing to delete or re-index, the single array element is the default block count
			if (priorData == null || priorData.length <= 1)		 
				continue; 

			postSyncJob(md.getCluster(), md.getId(), (initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, priorData);
		}
		return true;
	}

	//array of data type, data frequency, & serialized data as byte[]
	@Override
	public Object[] readBlockData(MasterData md, long start, long end) throws TimeSeriesException {
		try {
			serializer.reset();
			String returnDataType = md.getDatatype();
			Frequency dataFrequency = md.getFreq();
			Schema.Type dataType = DynamicTypeTranslator.getSchemaType(returnDataType);
			serializer.resetSchema(dataType);
			serializer.setBlockCount(1);
			setSerializedData(md, start, end, this.serializer);
			return new Object[]{returnDataType, dataFrequency, serializer.getRawData()};
		} catch (Exception ex) {
			logger.error("Error inserting data",ex);
			throw new TimeSeriesException(ex.getMessage());	
		}
	}
	
	@Override
	public byte[] getSerializedData(MasterData md, SortedMap<Long, Object> data) throws TimeSeriesException {
		try {
			serializer.reset();
			String returnDataType = md.getDatatype();
			//Frequency dataFrequency = md.getFreq();
			Schema.Type dataType = DynamicTypeTranslator.getSchemaType(returnDataType);
			serializer.resetSchema(dataType);
			serializer.setBlockCount(1);
			if (data != null) {
				serializer.startNewBlock(md.getCluster(), md.getLn(), md.getGuid(), md.getStartts(), data.size());
				for (Long o : data.keySet()) {
					serializer.setData(o, data.get(o));
				}
			}
			return serializer.getRawData();
		} catch (Exception ex) {
			logger.error("Error inserting data",ex);
			throw new TimeSeriesException(ex.getMessage());	
		}
	}
	
	//array of data type, data frequency, & serialized data as byte[]
	@Override
	public Object[] readBlockData(String cluster, int ln, String guid, long begints, long beginoffset, long endts, long endoffset)
			throws TimeSeriesException {
		try {
			serializer.reset();
			List<MasterData> mdList = getMasterDataForBlocks(cluster, ln,guid,begints,endts);
			if (mdList == null || mdList.size() == 0)
				throw new TimeSeriesException("No master data records found between start time =" + begints + " and end time = " + endts + " for guid=" + guid);
			String returnDataType = mdList.get(0).getDatatype();
			Frequency dataFrequency = mdList.get(0).getFreq();
			Schema.Type dataType = DynamicTypeTranslator.getSchemaType(returnDataType);
			serializer.resetSchema(dataType);
			serializer.setBlockCount(mdList.size());
			MasterData md = null;
			for (int i = 0; i < mdList.size(); i++ ) {
				md = mdList.get(i);
				if (TimeSeriesShard.ignoreOffsets(md.getFreq())) {
					beginoffset = 0;
					endoffset = 0;
				}
				DateTime rowEndTs = TimeSeriesShard.getMaxEndDate(md.getFreq(), md.getStartts());
				long initialOffset = beginoffset;
				if (begints > md.getStartts())
					initialOffset = TimeSeriesShard.getOffset(md.getFreq(), begints);
				long finalOffset;
				if ((endts) < rowEndTs.getMillis())
					finalOffset = TimeSeriesShard.getOffset(md.getFreq(), endts);
				else
					finalOffset = TimeSeriesShard.getOffset(md.getFreq(), rowEndTs);
				setSerializedData(md, (initialOffset + beginoffset), (finalOffset + endoffset), this.serializer);
			}
			return new Object[]{returnDataType, dataFrequency, serializer.getRawData()};
		} catch (Exception ex) {
			logger.error("Error inserting data",ex);
			throw new TimeSeriesException(ex.getMessage());	
		}
	}
	
	@Override
	public SortedMap<Long,Object> fetchTimeSeries(MasterData md, long startOffset, long endOffset) 
			throws TimeSeriesException {
		try {
			return readBufferData(md, startOffset, endOffset);
		} catch (IOException e) {
			throw new TimeSeriesException(e);
		}
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException {
		return dhMixin.fetchTimeSeries(data);
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeriesLimit(MasterData md,
			long start, long end, int limit, boolean last) throws TimeSeriesException {
		try {
			return readBufferData(md, start, end, limit, last);
		} catch (IOException e) {
			throw new TimeSeriesException(e);
		}
	}

	@Override
	public void deleteNodeData(String cluster, int ln) throws TimeSeriesException {
		dhMixin.deleteNodeData(cluster, ln);
	}
	
	private void setSerializedData(MasterData md, long beginoffset, long endoffset, ITimeSeriesSerializer serializer) throws TimeSeriesException {
		 if (md == null)
			 return;
		 SortedMap<Long,Object> map = fetchTimeSeries(md,beginoffset, endoffset);
		if (map != null) {
			serializer.startNewBlock(md.getCluster(), md.getLn(), md.getGuid(), md.getStartts(), map.size());
			for (Long o : map.keySet()) {
				serializer.setData(o, map.get(o));
			}
		}
	}

	private void writeBufferData(MasterData md, SortedMap<Long,Object> data) throws TimeSeriesException {
			List<SortedMap<Long,Object>> splits = splitData(data, dataThreads);
			for (SortedMap<Long,Object> split : splits) {
				dataExecutorService.execute( (new Runnable(){
					private MasterData md = null;
					private SortedMap<Long,Object> data = null;
					@Override
					public void run() {
						List<Integer> updatedShards = new ArrayList<Integer>();
						int size = DataUtility.getByteSize(md.getDatatype()) + 1;
						try {
							for (Long i : data.keySet()) {
								int shardNum = getShard(md).getShardNumber(i);
								int index = (int) (i*size - getShard(md).getShardStart(i));
								put(getBuffer(md, shardNum),index, data.get(i), md.getDatatype());
								if (!updatedShards.contains(shardNum))
									updatedShards.add(shardNum);
							}
							for (int shardNum : updatedShards)
								getBuffer(md, shardNum).force();
						} catch (Exception ex) {
							logger.error("Error writing buffer data for master data " + md + " and split in offset range [" 
									+ data.firstKey() + "," + data.lastKey() + "]", ex);
						}
					}
					public Runnable initialize(MasterData md, SortedMap<Long,Object> data) {
						this.md = md;
						this.data = data;
						return this;
					}
				}).initialize(md, split));
			}
	}
	
/*	private void writeBufferDataOld(MasterData md, SortedMap<Long,Object> data) throws TimeSeriesException {
		try {
			List<Integer> updatedShards = new ArrayList<Integer>();
			int size = DataUtility.getByteSize(md.getDatatype()) + 1;
			for (Long i : data.keySet()) {
				int shardNum = getShard(md).getShardNumber(i);
				int index = (int) (i*size - getShard(md).getShardStart(i));
				put(getBuffer(md, shardNum),index, data.get(i), md.getDatatype());
				if (!updatedShards.contains(shardNum))
					updatedShards.add(shardNum);
			}
			for (int shardNum : updatedShards)
				getBuffer(md, shardNum).force();
		} catch (Exception ex) {
			throw new TimeSeriesException(ex);
		}
	}*/
	
/*	private void deleteBufferData(MasterData md, long start, long end) throws TimeSeriesException {
		try {
			List<Integer> updatedShards = new ArrayList<Integer>();
			int size = DataUtility.getByteSize(md.getDatatype()) + 1;
			for (long i = start; i <= end; i++) {
				int shardNum = getShard(md).getShardNumber(i);
				int index = (int) (i*size - getShard(md).getShardStart(i));
				reset(getBuffer(md, shardNum), index);
				if (!updatedShards.contains(shardNum))
					updatedShards.add(shardNum);
			}
			for (int shardNum : updatedShards)
				getBuffer(md, shardNum).force();
		} catch (Exception ex) {
		throw new TimeSeriesException(ex);
		}
	}*/
	
	private void deleteBufferData(MasterData md, long start, long end) throws TimeSeriesException {
		try {
			List<Integer> updatedShards = new ArrayList<Integer>();
			int size = DataUtility.getByteSize(md.getDatatype()) + 1;
			for (long i = start; i <= end; i++) {
				int shardNum = getShard(md).getShardNumber(i);
				int index = (int) (i*size - getShard(md).getShardStart(i));
				reset(getBuffer(md, shardNum), index);
				if (!updatedShards.contains(shardNum))
					updatedShards.add(shardNum);
			}
			for (int shardNum : updatedShards)
				getBuffer(md, shardNum).force();
		} catch (Exception ex) {
			throw new TimeSeriesException(ex);
		}
	}

	private MappedByteBuffer getBuffer(MasterData md, int shardNum) throws TimeSeriesException, IOException {
		if (!dataBuffers.containsKey(md.getId())) {
			dataBuffers.put(md.getId(), new TreeMap<Integer,MappedByteBuffer>());
		}
		if (!dataBuffers.get(md.getId()).containsKey(shardNum)) {
			long shardSize = getShard(md).getShardSize();
			long shardStart = shardSize*shardNum;
			File f = new File(storagePath + "/data" + "_" + md.getId());
			MappedByteBuffer map = getMappedBuffer(f, shardStart, shardSize);
			//System.out.println("Mapped Byte Buffer for file f = " + f + " and shardStart = " + shardStart +
			//		" and shardSize = " + shardSize + " = " + map);
			dataBuffers.get(md.getId()).put(shardNum, map);
		}
		return dataBuffers.get(md.getId()).get(shardNum);
	}
	
	public SortedMap<Long,Object> readBufferData(MasterData md, long start, long end) throws TimeSeriesException, IOException {
		return readBufferData(md, start, end, LocalNodeProperties.getSqlQueryLimit(), false);
	}
	
	public SortedMap<Long,Object> readBufferData(MasterData md, long start, long end, int limit, boolean last) throws TimeSeriesException, IOException {
		if (limit > LocalNodeProperties.getSqlQueryLimit())
			limit = LocalNodeProperties.getSqlQueryLimit();
		SortedMap<Long,Object> results = new TreeMap<Long,Object>();
		int size = DataUtility.getByteSize(md.getDatatype()) + 1;
		int counter = 0;
		long[] dataStatus = getDataStatus(md);
		FileShard shard = getShard(md);
		if (!last) {
			if (dataStatus[0] != -1 && start < dataStatus[0])
				start = dataStatus[0];
			for (long i = start; i <= end; i++) {
				int shardNum = shard.getShardNumber(i);
				int index = (int) (i*size - shard.getShardStart(i));
				Object o = get(getBuffer(md, shardNum),index, md.getDatatype());
				if (o != null) {
					results.put(i, o);
					++counter;
				}
				if (counter == limit)
					break;
			}
		} else {
			if (dataStatus[1] != -1 && end > dataStatus[1])
				end = dataStatus[1];
			long endOffset = shard.getTotalSize()/shard.getDataSize() - 1;
			if (end > endOffset)
				end = endOffset;
			for (long i = end; i >= start; i--) {
				int shardNum = shard.getShardNumber(i);
				int index = (int) (i*size - shard.getShardStart(i));
				Object o = get(getBuffer(md, shardNum),index, md.getDatatype());
				if (o != null) {
					results.put(i, o);
					++counter;
				}
				if (counter == limit)
					break;
			}
		}
		return results;
	}
	
	private List<SortedMap<Long,Object>> splitData(SortedMap<Long,Object> data, int numChunks) {
		List<SortedMap<Long,Object>> splits = new ArrayList<SortedMap<Long,Object>>();
		if (data.size() < 1000) {
			splits.add(data);
		} else {
			int chunkSize = data.size() / numChunks;
			List<Long> keylist = new ArrayList<Long>(data.keySet());
			for (int i = 0; i < data.size(); i += chunkSize) {
				splits.add(data.subMap(keylist.get(i), keylist.get(Math.min(data.size() - 1, i + chunkSize))));
			}
		}
		return splits;
	}
	
	private FileShard getShard(MasterData md) throws TimeSeriesException, IOException {
		if (!dataShards.containsKey(md.getId())) {
			int dataSize = DataUtility.getByteSize(md.getDatatype()) + 1;
			int offsetCount = (int) TimeSeriesShard.getOffsetCount(md);
			long totalSize = dataSize*1L*offsetCount;
			File f = new File(storagePath + "/data" + "_" + md.getId());
			if (!f.exists() || f.length() < totalSize) {
				synchronized(this) {
					preAllocateDataFile(f, md.getDatatype(), offsetCount);
					createSchema(md);
				}
			}
			int shardCount = ( (int) (totalSize / 100000000)) + 1;
			FileShard shard = new FileShard(dataSize, shardCount, totalSize);
			dataShards.put(md.getId(), shard);
		}
		return dataShards.get(md.getId());
	}
}
