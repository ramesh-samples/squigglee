// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.squigglee.coord.interfaces.IData;
import com.squigglee.coord.interfaces.ITask;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.CommandType;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.IndexingTask;
import com.squigglee.core.interfaces.SyncTask;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.ITimeSeriesSerializer;

public class DataHandlerImpl extends MasterDataHandlerImpl implements IDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.cassandra.DataHandlerImpl");
	private String insertData = "INSERT INTO " + TsrConstants.DATA_CF_NAME + " (id,offset,val) values (?, ?, ?);";
	//private String deleteData = "DELETE FROM " + TsrConstants.DATA_CF_NAME + " where id = ? and offset = ?;";
	private Map<String,PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
	//private Map<String,PreparedStatement> psMapDel = new HashMap<String, PreparedStatement>();
	
	@Override
	public boolean insertBulkData(byte[] bulkData) throws TimeSeriesException {
		if (bulkData == null || bulkData.length == 0)
			return true;
		ITask taskService = ServiceFactory.getTaskService();
		
		deserializer.setRawData(bulkData);
		//int count = 0;
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
				//long initialOffset = (long) Math.round((startts - masterData.getStartts()) * tickMultiplier);
				//if (!psMap.containsKey(masterData.getKs()))
				psMap.put(masterData.getKs(), getSession(masterData.getKs()).prepare(insertData));
				
				dataCount = deserializer.getDataCount(i);
				BatchStatement batch = new BatchStatement();
				for (int j = 0; j< dataCount; j++) 	{
					batch.add(psMap.get(masterData.getKs()).bind(masterData.getId(), (initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j)));
				}

				ResultSet batchResult = getSession().execute(batch);
				if (batchResult != null && batchResult.wasApplied()) {
					insertCounts[i] = dataCount;
					postIndexingJobs( ((int) (initialOffset + deserializer.getOffset(i,0)))
							, ((int) (initialOffset + deserializer.getOffset(i,(dataCount - 1)))), CommandType.INSERT, masterData, null);
				}
				batch.clear();
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				taskService.close();
				return false;
			}
		}
		//taskService.close();
		return true;
	}
	
	public void postIndexingJobs(long start, long end, CommandType operation, MasterData md, byte[] data) throws TimeSeriesException {
		ITask service = ServiceFactory.getTaskService();
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
		//service.close();
	}

	@Override
	public boolean updateBulkData(byte[] bulkData) throws TimeSeriesException {
		return insertBulkData(bulkData);
	}
	
	@Override
	public boolean deleteData(int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException {
		//ITask taskService = ServiceFactory.getTaskService();
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
				continue; 
			postIndexingJobs((initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, md, priorData);
			
			int batchSize = 50000;
			int batch = 0;
			BatchStatement batchStatement = new BatchStatement();
			Session session = getSession(md.getKs());
			for (long i = (initialOffset + beginoffset); i <= (finalOffset + endoffset); i++) {
				batchStatement.add(new SimpleStatement("delete from data where id = " + md.getId() + " and offset = " + i + ";"));
				batch++;
				if (batch == batchSize) {
					//ResultSet dataResult = 
					session.execute(batchStatement);
					batch = 0;
					batchStatement.clear();
				}
			}
			if (batchStatement.size() > 0) {
				//ResultSet dataResult = 
				session.execute(batchStatement);
					batchStatement.clear();
			}
		}
		
		//taskService.close();
		return true;
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
			serializer.resetSchema(dataType) ;
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
	public SortedMap<Long, Object> fetchTimeSeries(MasterData md,	long start, int startOffset, long end, int endOffset)
			throws TimeSeriesException {
		SortedMap<Long,Object> output = new TreeMap<Long,Object>();
		String cql = "select * from " + TsrConstants.DATA_CF_NAME + " WHERE id = " + md.getId() 
				+ " and offset <= " + end + " and offset >= " + start + " LIMIT 10000;"; 
		ResultSet dataResult = getSession(md.getKs()).execute(cql);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			for (Row row : rows)
				output.put(row.getLong("offset"), DynamicTypeTranslator.getDataVal(row,serializer.getDataType()));
		}

		return output;
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException {
		SortedMap<Long,Object> output = new TreeMap<Long,Object>();
		
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
					output.put((initialOffset + deserializer.getOffset(i,j)), deserializer.getVal(i,j));
		}
		return output;
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeriesLimit(MasterData md, long start, long end, int limit, boolean last) throws TimeSeriesException {
		SortedMap<Long,Object> output = new TreeMap<Long,Object>();
		String cql = "select * from " + TsrConstants.DATA_CF_NAME + " WHERE id = " + md.getId() 
				+ " and offset <= " + end + " and offset >= " + start + " order by offset " + (last?"desc":"asc") + " LIMIT " + limit + ";";
		//System.out.println(cql);
		ResultSet dataResult = getSession(md.getKs()).execute(cql);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			for (Row row : rows)
				output.put(row.getLong("offset"), DynamicTypeTranslator.getDataVal(row,serializer.getDataType()));
			//System.out.println();
		}

		return output;
	}

	@Override
	public void deleteNodeData(int ln) throws TimeSeriesException {
		IData dataService = ServiceFactory.getDataService();
		dataService.deleteNode(this.clusterName, ln);
		//dataService.close();
	}
	
	private void setSerializedData(MasterData md, long beginoffset, long endoffset, ITimeSeriesSerializer serializer) throws TimeSeriesException {
		 if (md == null)
			 return;
		//serializer.reset();
		//serializer.resetSchema(DynamicTypeTranslator.getSchemaType(list.get(0).getDatatype())) ;
		//serializer.setBlockCount(1);
		String select = "select id,offset,val from " + TsrConstants.DATA_CF_NAME + " where id = " + md.getId() + " and " +
				"offset >= " + (beginoffset) + " and offset <= " + (endoffset) + ";";
			
		ResultSet dataResult = getSession(md.getKs()).execute(select);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), rows.size());
			for (Row row : rows) {
				//boolean hasvalue = col.hasValue();
				Object dataVal = DynamicTypeTranslator.getDataVal(row, serializer.getDataType());
				serializer.setData(row.getLong("offset"), dataVal);
			}
		}
	}

	@Override
	public boolean syncData(SyncTask syncTask) throws TimeSeriesException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void updataSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void postIndexingJobs(SyncTask task) throws TimeSeriesException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean deleteData(MasterData md, long start, long end)
			throws TimeSeriesException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object[] readBlockData(MasterData md, long start, long end)
			throws TimeSeriesException {
		// TODO Auto-generated method stub
		return null;
	}


}
