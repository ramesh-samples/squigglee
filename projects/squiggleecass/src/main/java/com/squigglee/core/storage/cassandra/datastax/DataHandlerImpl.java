// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.squigglee.core.config.Frequency;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;

public class DataHandlerImpl extends MasterDataHandlerImpl implements IDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.DataHandlerImpl");
	private String insertData = "INSERT INTO " + TsrConstants.DATA_CF_NAME + " (id,offset,val) values (?, ?, ?);";
	private Map<String,PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
	public DataHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public int[] insertBulkData(byte[] bulkData) throws TimeSeriesException {
		if (bulkData == null || bulkData.length == 0)
			return null;
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
				if (batchResult != null && batchResult.wasApplied())
					insertCounts[i] = dataCount;
				batch.clear();
			} catch (Exception ex) {
				logger.error("Error inserting data",ex);
				throw new TimeSeriesException(ex.getMessage() + " for guid = " + deserializer.getGuid(i) + " and startts = " 
						+ new DateTime(deserializer.getStartts(i),DateTimeZone.UTC));
			}
			//insertCounts[i] = dataCount;
		}
		return insertCounts;
	}

	@Override
	public int[] updateBulkData(byte[] bulkData) throws TimeSeriesException {
		return insertBulkData(bulkData);
	}

	@Override
	public void deleteData(int ln, String guid, long begints, long beginoffset, long endts, long endoffset) throws TimeSeriesException {
		// TODO Auto-generated method stub
		//return null;
	}

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

				String select = "select id,offset,val from " + TsrConstants.DATA_CF_NAME + " where id = " + md.getId() + " and " +
					"offset >= " + (initialOffset + beginoffset) + " and offset <= " + (finalOffset + endoffset) + ";";
				
				ResultSet dataResult = getSession(md.getKs()).execute(select);
				if (dataResult != null) {
					List<Row> rows = dataResult.all();
					
						if (begints > md.getStartts())
							serializer.startNewBlock(md.getLn(), md.getGuid(), begints, rows.size());
						else
							serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), rows.size());
						
						for (Row row : rows) {
							//boolean hasvalue = col.hasValue();
							Object dataVal = DynamicTypeTranslator.getDataVal(row, serializer.getDataType());
							serializer.setData(row.getLong("offset") - initialOffset,	dataVal);
						}
					}

			}
			return new Object[]{returnDataType, dataFrequency, serializer.getRawData()};
		} catch (Exception ex) {
			logger.error("Error inserting data",ex);
			throw new TimeSeriesException(ex.getMessage());	
		}
	}

	@Override
	public SortedMap<Object, Object> fetchTimeSeries(long id, String dataType,
			int start, int startOffset, int end, int endOffset)
			throws TimeSeriesException {
		SortedMap<Object,Object> output = new TreeMap<Object,Object>();
		String cql = "select * from " + TsrConstants.DATA_CF_NAME + " WHERE id = " + id 
				+ " and offset <= " + end + " and offset >= " + start + " LIMIT 10000;"; 
		ResultSet dataResult = getSession(dataType).execute(cql);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			for (Row row : rows)
				output.put((int) row.getLong("offset"), DynamicTypeTranslator.getDataVal(row,serializer.getDataType()));
		}

		return output;
	}
	
	@Override
	public SortedMap<Integer, Object> fetchTimeSeriesLimit(long id, String dataKeyspace,
			int start, int end, int limit, boolean last) throws TimeSeriesException {
		SortedMap<Integer,Object> output = new TreeMap<Integer,Object>();
		String cql = "select * from " + TsrConstants.DATA_CF_NAME + " WHERE id = " + id 
				+ " and offset <= " + end + " and offset >= " + start + " order by offset " + (last?"desc":"asc") + " LIMIT " + limit + ";";
		//System.out.println(cql);
		ResultSet dataResult = getSession(dataKeyspace).execute(cql);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			for (Row row : rows)
				output.put((int) row.getLong("offset"), DynamicTypeTranslator.getDataVal(row,serializer.getDataType()));
			//System.out.println();
		}

		return output;
	}


	@Override
	public void deleteNodeData(int ln) throws TimeSeriesException {
		
		String cql = "select id,ks,ln,guid,startts,freq,datatype,replication,strategy,indexes from " 
				+ TsrConstants.MASTER_DATA_CF_NAME + " WHERE LN = " + ln + " ALLOW FILTERING;";
		List<MasterData> list = new ArrayList<MasterData>();
		List<String> dataKeyspaces = new ArrayList<String>();
		String idList = "";
		ResultSet dataResult = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		if (dataResult != null) {
			List<Row> rows = dataResult.all();
			for (Row row : rows) {		
				list.add( new MasterData (
						row.getLong("id"),
						row.getString("ks"),
						row.getInt("ln"),
						row.getString("guid"),
						row.getLong("startts"),
						Frequency.valueOf(row.getString("freq")),
						row.getString("datatype"),
						row.getString("replication"),
						row.getString("strategy"),
						row.getString("indexes")
					));
				idList += row.getLong("id") + ",";
				if (!dataKeyspaces.contains(row.getString("ks")))
					dataKeyspaces.add(row.getString("ks"));
			}
		}
		if (idList.endsWith(","))
			idList = idList.substring(0,idList.length()-1);
		for (String ks : dataKeyspaces)
			deleteSchema(ks);
		
		String deleteCql = "DELETE FROM " + TsrConstants.MASTER_DATA_CF_NAME + " WHERE ID IN (" + idList + ")";
		//ResultSet deleteResult = 
		getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(deleteCql);
		
	}

	@Override
	public SortedMap<Integer, Object> fetchTimeSeries(byte[] data)
			throws TimeSeriesException {
		// TODO Auto-generated method stub
		return null;
	}

}
