// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.squigglee.coord.storage.DataHandlerMixin;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.interfaces.CommandType;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.SyncTask;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.ITimeSeriesSerializer;

public class DataHandlerImpl extends MasterDataHandlerImpl implements IDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.DataHandlerImpl");
	protected DataHandlerMixin dhMixin = null;
	public DataHandlerImpl() {
		super();
		dhMixin = new DataHandlerMixin(this.clusterName, ln, this.deserializer, this.serializer);
	}
	
	@Override
	public boolean insertBulkData(byte[] bulkData) throws TimeSeriesException {
		if (bulkData == null || bulkData.length == 0)
			return false;
		//ITask taskService = ServiceFactory.getTaskService();
		//ISync syncService = ServiceFactory.getSyncService();
		//int[] results = null;
		deserializer.setRawData(bulkData);
		int[] insertCounts = new int[deserializer.getBlockCount()];
		for (int i = 0; i< insertCounts.length; i++) {
			int dataCount = 0;
			try {
				long startts = deserializer.getStartts(i);
				MasterData md = getMasterData(deserializer.getLn(i),deserializer.getGuid(i),startts);				
				if (md == null)
					throw new TimeSeriesException("No master data found for requested insert for ln = " + deserializer.getLn(i) + 
							" and guid = " + deserializer.getGuid(i) + " and startts = " + startts);
				long initialOffset = 0;
				if (startts > md.getStartts())
					initialOffset = TimeSeriesShard.getOffset(md.getFreq(), startts);
				dataCount = deserializer.getDataCount(i);
				deleteData(md, (initialOffset + deserializer.getOffset(i,0)), (initialOffset + deserializer.getOffset(i,(dataCount - 1))));
								
				postSyncJob(md.getId(), (initialOffset + deserializer.getOffset(i,0))
						, (initialOffset + deserializer.getOffset(i,(dataCount - 1))), CommandType.INSERT, bulkData);
				
				//postIndexingJobs( ((int) (initialOffset + deserializer.getOffset(i,0)))
				//		, ((int) (initialOffset + deserializer.getOffset(i,(dataCount - 1)))), CommandType.INSERT, md, null);
				
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				//taskService.close();
				//syncService.close();
				return false;
			}
		}
		//taskService.close();
		//syncService.close();
		return true;
	}
	
	public void postSyncJob(long id, long start, long end, CommandType operation, byte[] data) throws TimeSeriesException {
		dhMixin.postSyncJob(id, start, end, operation, data);
		/*
		ISync syncService = ServiceFactory.getSyncService();
		SyncTask syncTask = new SyncTask(this.clusterName, id, start, end, operation, data);
		syncService.addSync(syncTask);
		//syncService.close();
		*/
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
		//int[] results = null;
		deserializer.setRawData(syncTask.getData());
		int[] insertCounts = new int[deserializer.getBlockCount()];
		for (int i = 0; i< insertCounts.length; i++) {
			int dataCount = 0;
			try {
				long startts = deserializer.getStartts(i);
				MasterData masterData = getMasterData(deserializer.getLn(i),deserializer.getGuid(i),startts);
				if (masterData == null)
					throw new TimeSeriesException("No master data found for requested insert");
				Type dataType = DynamicTypeTranslator.getSchemaType(masterData.getDatatype());
				
				deserializer.resetSchema(dataType);
				deserializer.setRawData(syncTask.getData());
				long initialOffset = 0;
				if (startts > masterData.getStartts())
					initialOffset = TimeSeriesShard.getOffset(masterData.getFreq(), startts);
				dataCount = deserializer.getDataCount(i);
				Connection conn = getConnection();
				//PreparedStatement st = conn.prepareStatement("MERGE INTO " + masterData.getKs() + "_" + masterData.getId() + " (off,val) KEY (off) values (?,?)");
				PreparedStatement st = conn.prepareStatement("INSERT INTO " + masterData.getKs() + "_" + masterData.getId() + " (off,val) values (?,?)");
				
				for (int j = 0; j< dataCount; j++) 	{
					//System.out.println(" i = " + i + " j = " + j + " offset = " + deserializer.getOffset(i,j) + " val = " + deserializer.getVal(i,j));
					st.setLong(1, (initialOffset + deserializer.getOffset(i,j)));
					//st.setObject(2, deserializer.getVal(i,j));
					DynamicTypeTranslator.setPreparedStatementValue(2, st, deserializer.getVal(i,j), DynamicTypeTranslator.getSchemaType(masterData.getDatatype()));
					st.addBatch();
				}
				//results = 
						st.executeBatch();
				if (conn != null && !conn.isClosed())
					conn.close();

				result = true;
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				throw new TimeSeriesException(ex.getMessage() + " for guid = " + deserializer.getGuid(i) + " and startts = " 
						+ new DateTime(deserializer.getStartts(i),DateTimeZone.UTC));
			}
		}
		//System.out.println("Posting indexing task from syncInsertData " + syncTask);
		//postIndexingJobs(syncTask);
		return result;
	}
	
	private boolean syncDeleteData(SyncTask syncTask) throws TimeSeriesException {
		List<MasterData> list = getMasterData(syncTask.getId());
		boolean result = false;
		if (list == null || list.isEmpty())
			return false;
		MasterData md = list.get(0);
		Connection conn = getConnection();
		PreparedStatement st;
		try {
			st = conn.prepareStatement("DELETE FROM " + md.getKs() + "_" + md.getId() + " where off = ?");
			for (long i = syncTask.getStartoffset(); i <= syncTask.getEndoffset(); i++) {
				st.setLong(1, i);
				st.addBatch();
			}
			st.executeBatch();
			if (conn != null && !conn.isClosed())
				conn.close();

			result = true;
		} catch (SQLException e) {
			logger.error("Error deleting data for ln = " + ln + " and guid = " + md.getGuid() + " for range [" 
				+ syncTask.getStartoffset() + "," + syncTask.getEndoffset(), e);
			result = false;
		}
		//System.out.println("Posting indexing task from syncDeleteData " + syncTask);
		//postIndexingJobs(syncTask);	
		return result;
	}

	@Override
	public void updataSyncStatus(SyncTask syncTask) throws TimeSeriesException {
		dhMixin.updataSyncStatus(syncTask);
		/*
		ISync syncService = ServiceFactory.getSyncService();
		syncService.logSync(syncTask, ln);	
		//syncService.close();
		 */
	}
	
	@Override
	public void postIndexingJobs(SyncTask syncTask) throws TimeSeriesException {
		dhMixin.postIndexingJobs(syncTask);
		/*
		//System.out.println("In postIndexingJobs method");
		ITask service = ServiceFactory.getTaskService();
		List<MasterData> list = getMasterData(syncTask.getId());
		if (list == null || list.isEmpty())
			return;
		MasterData md = list.get(0);
		
		Map<IndexType,Integer> maxDims = md.getMaxIndexDimension();
		if (maxDims.isEmpty())
			return;		// no indexes configured
		for (String index : md.getIndexes().split(";")) {
			String[] tokens = index.split("_"); 
			IndexType it = IndexType.valueOf(tokens[0]);
			long st = (syncTask.getStartoffset() - maxDims.get(it));
			long et = (syncTask.getEndoffset() + maxDims.get(it));
			
			if (it.equals(IndexType.ptrn)) {	//load balance the pattern indexes across the replica set
				for (int destinationLn : service.getReplicaSet(clusterName, md.getId())) {
					IndexingTask task = new IndexingTask(this.clusterName, destinationLn, md.getId(), (st < 0 ? 0 : st), 
							(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, syncTask.getDataOperation());
					if (syncTask.getDataOperation().equals(CommandType.DELETE) || syncTask.getDataOperation().equals(CommandType.UPDATE)) {
						if (syncTask.getData() != null && syncTask.getData().length > 0)
							task.setPriorData(syncTask.getData());
					}
					service.addTask(task);
				}
			} else {	// run sketches at a single location
				IndexingTask task = new IndexingTask(this.clusterName, md.getLn(), md.getId(), (st < 0 ? 0 : st), 
						(et >= TsrConstants.COLUMN_FAMILY_MAX_COLUMNS ? TsrConstants.COLUMN_FAMILY_MAX_COLUMNS : et), index, syncTask.getDataOperation());
				if (syncTask.getDataOperation().equals(CommandType.DELETE) || syncTask.getDataOperation().equals(CommandType.UPDATE)) {
					if (syncTask.getData() != null && syncTask.getData().length > 0) {
						task.setPriorData(syncTask.getData());
					}
				}
				service.addTask(task);
			}
		}
		//service.close();
		*/
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

		postSyncJob(md.getId(), start, end, CommandType.DELETE, priorData);
		
		return result;
	}
	
	@Override
	public boolean deleteData(int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException {
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
			// nothing to delete or re-index, the single array element is the default block count
			if (priorData == null || priorData.length <= 1)		 
				continue; 

			postSyncJob(md.getId(), (initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, priorData);
			
			/*
			Connection conn = getConnection();
			PreparedStatement st;
			try {
				st = conn.prepareStatement("DELETE FROM " + md.getKs() + "_" + md.getId() + " where off = ?");
				for (long i = (initialOffset + beginoffset); i <= (finalOffset + endoffset); i++) {
					st.setLong(1, i);
					st.addBatch();
					//map.remove(i);
				}
				//int[] results = 
					st.executeBatch();
				if (conn != null && !conn.isClosed())
					conn.close();
					
			
				//need better error handling here
				postIndexingJobs((initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, taskService, md, priorData);	
				postSyncJob(md.getId(), (initialOffset + beginoffset), (finalOffset + endoffset), CommandType.DELETE, priorData);

			} catch (SQLException e) {
				logger.error("Error deleting data for ln = " + ln + " and guid = " + guid + " for range [" 
					+ (initialOffset + beginoffset) + "," + (finalOffset + endoffset), e);
				taskService.close();
				syncService.close();
				return false;
			}
			*/
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
	public SortedMap<Long,Object> fetchTimeSeries(MasterData md,
			long start, int startOffset, long end, int endOffset) throws TimeSeriesException {
		SortedMap<Long,Object> ts = new TreeMap<Long, Object>();
		
		Connection conn = getConnection();
		String query = "SELECT off, val from " + md.getKs() + "_" + md.getId() + " where off >= " + start + " and off <= " + end;
		//MVStore s = getStore("" + md.getId());
		//MVMap<Object, Object> data = getMap(s, md.getDatatype());
		//DB db = getDatabase("" + id);
		//SortedMap<Object,Object> data = getTable(db, dataType).subMap(new Long(start), true, new Long(end), true);
		ResultSet rs;
		try {
			rs = conn.createStatement().executeQuery(query);
			while (rs.next()) {
				ts.put(rs.getLong("off"), rs.getObject("val"));
			}
			if (rs != null && !rs.isClosed())
				rs.close();
			if (conn != null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//for (Object key : data.keySet())
		//	ts.put(new Long(key.toString()), DynamicTypeTranslator.parseStringObject(data.get(key).toString(), md.getDatatype()) );
		//s.close();
		//db.close();
		
		return ts;
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeries(byte[] data) throws TimeSeriesException {
		
		return dhMixin.fetchTimeSeries(data);
		/*
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
		*/
	}
	
	@Override
	public SortedMap<Long, Object> fetchTimeSeriesLimit(MasterData md,
			long start, long end, int limit, boolean last) throws TimeSeriesException {
		SortedMap<Long,Object> output = new TreeMap<Long,Object>();
		
		Connection conn = getConnection();
		
		String query = last ? ("SELECT off, val from " + md.getKs() + "_" + md.getId() + " where off <= " + end + " ORDER BY off desc LIMIT " + limit)
						: ("SELECT off, val from " + md.getKs() + "_" + md.getId() + " where off >= " + start + " ORDER BY off asc LIMIT " + limit);
		
		ResultSet rs;
		try {
			rs = conn.createStatement().executeQuery(query);
			while (rs.next()) {
				output.put(rs.getLong("off"), rs.getObject("val"));
			}
			if (rs != null && !rs.isClosed())
				rs.close();
			if (conn != null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output;
	}

	@Override
	public void deleteNodeData(int ln) throws TimeSeriesException {
		dhMixin.deleteNodeData(ln);
		/*
		IData dataService = ServiceFactory.getDataService();
		dataService.deleteNode(this.clusterName, ln);
		//dataService.close();
		 */
	}
	
	private void setSerializedData(MasterData md, long beginoffset, long endoffset, ITimeSeriesSerializer serializer) throws TimeSeriesException {
		 if (md == null)
			 return;
		//serializer.reset();
		//serializer.resetSchema(DynamicTypeTranslator.getSchemaType(list.get(0).getDatatype())) ;
		//serializer.setBlockCount(1);
		 
		 SortedMap<Long,Object> map = fetchTimeSeries(md, (int) beginoffset, 0, (int) endoffset, 0);
		 
		if (map != null) {
			serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), map.size());
			for (Long o : map.keySet()) {
				serializer.setData(o, map.get(o));
			}
		}
	}



}
