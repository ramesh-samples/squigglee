// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.vdb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mapdb.DB;

import com.squigglee.coord.interfaces.IData;
import com.squigglee.coord.interfaces.ITask;
import com.squigglee.coord.interfaces.IndexingTask;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.CommandType;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.ITimeSeriesSerializer;

public class DataHandlerImpl extends MasterDataHandlerImpl implements IDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.vdb.DataHandlerImpl");
	public DataHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public int[] insertBulkData(byte[] bulkData) throws TimeSeriesException {
		if (bulkData == null || bulkData.length == 0)
			return null;
		ITask taskService = serviceFactory.getTaskService();
		
		deserializer.setRawData(bulkData);
		int[] insertCounts = new int[deserializer.getBlockCount()];
		for (int i = 0; i< insertCounts.length; i++) {
			int dataCount = 0;
			try {
				long startts = deserializer.getStartts(i);
				MasterData masterData = getMasterData(deserializer.getLn(i),deserializer.getGuid(i),startts);				
				if (masterData == null)
					throw new TimeSeriesException("No master data found for requested insert");
				//double tickMultiplier = TimeSeriesShard.getTickMultiplier(masterData.getFreq());
				long initialOffset = 0;
				if (startts > masterData.getStartts())
					initialOffset = TimeSeriesShard.getOffset(masterData.getFreq(), startts);
				dataCount = deserializer.getDataCount(i);
				//DB db = getDatabase("" + masterData.getId());
				MVStore s = getStore("" + masterData.getId());
				
				MVMap<Object, Object> map = getMap(s, masterData.getDatatype());
				//ConcurrentNavigableMap<Object,Object> map = getTable(db, masterData.getDatatype());
				
				//safest, but slower, to delete first before inserting
				deleteData(masterData.getLn(), masterData.getGuid(), (startts + deserializer.getOffset(i,0)), 0,
						(startts + deserializer.getOffset(i,(dataCount - 1))), 0);
				
				for (int j = 0; j< dataCount; j++) 	{
					map.put( (initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j) );
				}
				s.commit();
				//s.close();
				//db.commit();
				//db.close();
				insertCounts[0] = dataCount;
				
				postIndexingJobs( ((int) (initialOffset + deserializer.getOffset(i,0)))
						, ((int) (initialOffset + deserializer.getOffset(i,(dataCount - 1)))), CommandType.INSERT, taskService, masterData, null);
				
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				throw new TimeSeriesException(ex.getMessage() + " for guid = " + deserializer.getGuid(i) + " and startts = " 
						+ new DateTime(deserializer.getStartts(i),DateTimeZone.UTC));
			}
		}
		taskService.close();
		return insertCounts;
	}
	
	private void postIndexingJobs(long start, long end, CommandType operation, ITask service, MasterData md, byte[] data) {
		Map<IndexType,Integer> maxDims = md.getMaxIndexDimension();
		if (maxDims.isEmpty())
			return;		// no indexes configured
		for (String index : md.getIndexes().split(";")) {
			String[] tokens = index.split("_"); 
			IndexType it = IndexType.valueOf(tokens[0]);
			long st = (start - maxDims.get(it));
			long et = (end + maxDims.get(it));
			
			if (it.equals(IndexType.ptrn)) {	//load balance the pattern indexes across the replica set
				for (int destinationLn : service.getReplicaSet(clusterName, md.getId())) {
					IndexingTask task = new IndexingTask(this.clusterName, destinationLn, md.getId(), (st < 0 ? 0 : st), 
							(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, operation);
					if (operation.equals(CommandType.DELETE) || operation.equals(CommandType.UPDATE)) {
						if (data != null && data.length > 0)
							task.setPriorData(data);
					}
					service.addTask(task);
				}
			} else {	// run sketches at a single location
				IndexingTask task = new IndexingTask(this.clusterName, md.getLn(), md.getId(), (st < 0 ? 0 : st), 
						(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, operation);
				if (operation.equals(CommandType.DELETE) || operation.equals(CommandType.UPDATE)) {
					if (data != null && data.length > 0) {
						task.setPriorData(data);
					}
				}
				service.addTask(task);
			}
		}
	}

	@Override
	public int[] updateBulkData(byte[] bulkData) throws TimeSeriesException {
		return insertBulkData(bulkData);
	}
	
	@Override
	public void deleteData(int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException {
		ITask taskService = serviceFactory.getTaskService();
		for (MasterData md : getMasterData(ln, guid, begints, endts)) {
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
			
			byte[] priorData = (byte[]) readBlockData(ln, guid, begints, beginoffset, endts, endoffset)[2];
			if (priorData == null || priorData.length <= 1)		// nothing to delete, the single array element is the default block count 
				return; 
			
			//need better error handling here
			postIndexingJobs((initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, taskService, md, priorData);	
			MVStore s = getStore("" + md.getId());
			MVMap<Object, Object> map = getMap(s, md.getDatatype());
			
			//DB db = getDatabase("" + md.getId());
			
			//ConcurrentNavigableMap<Object,Object> map = getTable(db, md.getDatatype());
			
			for (long i = (initialOffset + beginoffset); i <= (finalOffset + endoffset); i++)
				map.remove(i);
			s.commit();
			//s.close();
			
			//db.commit();
			//db.close();
		}
		taskService.close();
	}

	//array of data type, data frequency, & serialized data as byte[]
	@Override
	public Object[] readBlockData(int ln, String guid, long begints, long beginoffset, long endts, long endoffset)
			throws TimeSeriesException {
		try {
			serializer.reset();
			List<MasterData> mdList = getMasterDataForBlocks(ln,guid,begints,endts);
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
	public SortedMap<Object,Object> fetchTimeSeries(long id, String dataType,
			long start, int startOffset, long end, int endOffset) throws TimeSeriesException {
		SortedMap<Object,Object> ts = new TreeMap<Object, Object>();
		
		MVStore s = getStore("" + id);
		MVMap<Object, Object> data = getMap(s, dataType);
		//DB db = getDatabase("" + id);
		//SortedMap<Object,Object> data = getTable(db, dataType).subMap(new Long(start), true, new Long(end), true);
		
		
		
		for (Object key : data.keySet())
			ts.put(new Long(key.toString()), DynamicTypeTranslator.parseStringObject(data.get(key).toString(), dataType) );
		//s.close();
		//db.close();
		
		return ts;
	}
	
	@Override
	public SortedMap<Integer, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException {
		SortedMap<Integer,Object> output = new TreeMap<Integer,Object>();
		
		deserializer.setRawData(data);
		int blockCount = deserializer.getBlockCount();
		if (blockCount > 1)
			logger.info("Serialized data has multiple blocks");
		int[] insertCounts = new int[blockCount];
		
		for (int i = 0; i< insertCounts.length; i++) {
				int dataCount = 0;
				long startts = deserializer.getStartts(i);
				MasterData masterData = getMasterData(deserializer.getLn(i),deserializer.getGuid(i),startts);
				if (masterData == null)
					throw new TimeSeriesException("No master data found for serialized data ");
				long initialOffset = 0;
				if (startts > masterData.getStartts())
					initialOffset = TimeSeriesShard.getOffset(masterData.getFreq(), startts);				
				dataCount = deserializer.getDataCount(i);
				for (int j = 0; j< dataCount; j++)
					output.put((int) (initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j));
		}
		return output;
	}
	
	@Override
	public SortedMap<Integer, Object> fetchTimeSeriesLimit(long id, String dataType,
			long start, long end, int limit, boolean last) throws TimeSeriesException {
		SortedMap<Integer,Object> output = new TreeMap<Integer,Object>();
		List<MasterData> list = getMasterData(id);
		if (list == null || list.size() == 0)
			return null;
		
		MVStore s = getStore("" + id);
		MVMap<Object, Object> map = getMap(s, dataType);
		
		//DB db = getDatabase("" + id);
		//ConcurrentNavigableMap<Object,Object> map = getTable(db, list.get(0).getDatatype());
		
		int count = 0;
		if (last)
			Collections.reverse(map.keyList());
		for (Object l : map.keyList()) {
			output.put(Integer.parseInt(l.toString()), DynamicTypeTranslator.parseStringObject(map.get(l).toString(), dataType));
			if (count++ == limit)
				break;
		}
		//s.close();
		//db.close();
		return output;
	}

	@Override
	public void deleteNodeData(int ln) throws TimeSeriesException {
		IData dataService = serviceFactory.getDataService();
		dataService.deleteNode(this.clusterName, ln);
		dataService.close();
	}
	
	private void setSerializedData(MasterData md, long beginoffset, long endoffset, ITimeSeriesSerializer serializer) throws TimeSeriesException {
		 if (md == null)
			 return;
		//serializer.reset();
		//serializer.resetSchema(DynamicTypeTranslator.getSchemaType(list.get(0).getDatatype())) ;
		//serializer.setBlockCount(1);
		 
		 SortedMap<Object,Object> map = fetchTimeSeries(md.getId(), md.getDatatype(), (int) beginoffset, 0, (int) endoffset, 0);
		 
		if (map != null) {
			serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), map.size());
			for (Object o : map.keySet()) {
				serializer.setData(Long.parseLong(o.toString()), map.get(o));
			}
		}
	}
}
