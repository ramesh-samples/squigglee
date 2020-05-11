package com.squigglee.api.restproxy;


import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.NodeStatus;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;

public class TimeSeriesProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.TimeSeriesProxy");
	protected int localLn = 0;
	protected int queryLimit = 0;
	protected String localCluster = null;
	protected IDataHandler dataHandler = null;
	
	public TimeSeriesProxy(IDataHandler dataHandler, int localLn, String localCluster, int limit) {
		this.dataHandler = dataHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
		this.queryLimit = limit;
	}

	public SortedMap<Long,Object> getData(String cluster, int dataln, String guid, long start, int startoffset, long end, int endoffset) {
		return getData(cluster, dataln, guid, start, startoffset, end, endoffset, queryLimit, false);
	}
	
	public SortedMap<Long,Object> getData(String cluster, int dataln, String guid, long start, int startHfOffset, long end, int endHfOffset, int limit, boolean last) {
		SortedMap<Long,Object> data = new TreeMap<Long,Object>();
		try {
			if (limit > queryLimit)
				limit = queryLimit;
			List<MasterData> list = dataHandler.getMasterData(cluster, dataln, guid, start, end);
			SortedMap<Long,Object> results = null;
			for (MasterData md : list) {
				if (dataHandler.getReplicaSet(md.getCluster(), md.getLn()).contains(localLn)) {
					int startOffset, endOffset;
					if (TimeSeriesShard.ignoreOffsets(md.getFreq())) {
						startOffset = (int) TimeSeriesShard.getOffset(md.getFreq(), start);
						endOffset = (int) TimeSeriesShard.getOffset(md.getFreq(), end);
					} else {
						startOffset = (int) TimeSeriesShard.getOffset(md.getFreq(), start, startHfOffset);
						endOffset = (int) TimeSeriesShard.getOffset(md.getFreq(), end, endHfOffset);
					}
					results = dataHandler.fetchTimeSeriesLimit(md, startOffset, endOffset, limit, last);
				}
				else {
					NodeStatus alternateLocation = dataHandler.getAlternateLocation(md.getCluster(), md.getLn());
					logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
					
					System.out.println("Making proxy request for data id = " + md.getId() + " guid = " + md.getGuid() + " startts = " + start +
							" start hf offset " + startHfOffset + " endts " + end + " end hf offset " + endHfOffset +
							" for " + (last?"last ":"first ") + limit + " values " + " for ln = " + md.getLn() + " in cluster " + md.getCluster());
					logger.debug("Making proxy request for data id = " + md.getId() + " guid = " + md.getGuid() + " startts = " + start +
							" start hf offset " + startHfOffset + " endts " + end + " end hf offset " + endHfOffset +
							" for " + (last?"last ":"first ") + limit + " values " + " for ln = " + md.getLn() + " in cluster " + md.getCluster());
					TimeSeries proxyTs = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).getSequencedTimeSeriesJSON
							(md.getCluster(), md.getLn(), md.getGuid(), start, startHfOffset, end, endHfOffset, limit, last);
					results = proxyTs.getData();
				}
				//results = dataHandler.fetchTimeSeriesLimit(md, 
				//		(start - md.getStartts()), (end - md.getStartts()), queryLimit, last);
				if (results == null | results.size() == 0)
					continue;
				if (data.size() + results.size() > limit) {
					for (Long key : results.keySet()) {
						long ts = md.getStartts() + key;
						if (!TimeSeriesShard.ignoreOffsets(md.getFreq())) {
							ts = key;
						}
						data.put(ts, results.get(key));
						if (data.size() >= limit)
							break;
					}
				} else {
					data.putAll(results);
				}
				if (data.size() >= limit)
					break;
			}
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching time series for ln = " + dataln + " and id = " + guid);
		}
		return data;
	}
	
	public TimeSeries getData(String cluster, int dataln, String id, long startts, int startOffset, int endOffset, int limit, boolean last) {
		TimeSeries ts = new TimeSeries();
		try {
			MasterData md = dataHandler.getMasterData(cluster, dataln, id, startts);
			if (limit > queryLimit)
				limit = queryLimit;
				if (dataHandler.getReplicaSet(cluster, dataln).contains(localLn)) {
					SortedMap<Long,Object> data = dataHandler.fetchTimeSeriesLimit(md, startOffset, endOffset, limit, last);
					if (!data.isEmpty()) {
					if (TimeSeriesShard.ignoreOffsets(md.getFreq()))
						ts = new TimeSeries(md.getCluster(), md.getLn(), md.getGuid(), md.getStartts() + (data.isEmpty()?0L:data.firstKey()), 
								0, md.getStartts() + (data.isEmpty()?0L:data.lastKey()), 0);
					else
						ts = new TimeSeries(md.getCluster(), md.getLn(), md.getGuid(), md.getStartts(), (int) (data.isEmpty()?0L:data.firstKey()), 
								md.getStartts(), (int) (data.isEmpty()?0L:data.lastKey()));
					}
					ts.setData(data);
				}
				else {
					NodeStatus alternateLocation = dataHandler.getAlternateLocation(cluster, dataln);
					logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
					
					System.out.println("Making proxy request for data id = " + md.getId() + " guid = " + md.getGuid() + " start offset = " + startOffset +
							" and end offset " + endOffset + " endts " + " for " + (last?"last ":"first ") + limit + " values " 
							+ " for ln = " + md.getLn() + " in cluster " + md.getCluster());
					logger.debug("Making proxy request for data id = " + md.getId() + " guid = " + md.getGuid() + " start offset = " + startOffset +
							" and end offset " + endOffset + " endts " + " for " + (last?"last ":"first ") + limit + " values " 
							+ " for ln = " + md.getLn() + " in cluster " + md.getCluster());
					
					ts = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).getSequencedTimeSeriesJSON
							(cluster, dataln, id, startts, startOffset, endOffset, limit, last);
				}
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching time series for startts = " + startts + " guid = " + id + " start offset = " + startOffset +
							" and end offset " + endOffset + " endts " + " for " + (last?"last ":"first ") + limit + " values " 
							+ " for ln = " + dataln + " in cluster " + cluster);
			ts.setCluster(cluster);
			ts.setLn(dataln);
			ts.setId(id);
			ts.setErrorMessage(e.getMessage());
		}
		return ts;
	}
	
	public TimeSeries getBulkData(String cluster, int dataln, String id, long start, int startHfOffset, long end, int endHfOffset) {
		TimeSeries ts = new TimeSeries(cluster, dataln, id, start, startHfOffset, end, endHfOffset);
		logger.debug("Received bulk post request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		System.out.println("Received bulk post request for time series for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
				+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
		try {
			if (dataHandler.getReplicaSet(ts.getCluster(), ts.getLn()).contains(localLn)) {
				try {
					Object[] results = dataHandler.readBlockData(ts.getCluster(), ts.getLn(), ts.getId(), ts.getStart(), ts.getStartHfOffset(), ts.getEnd(), ts.getEndHfOffset());
					if (results != null && results.length == 3 && results[2] != null)
						ts.setBulkData(RESTFactory.encode( (byte[]) results[2])); 
					System.out.println("Executed local bulk post request for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
							+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
					logger.debug("Executed local bulk post request for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
							+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
				} catch (TimeSeriesException e) {
					logger.error("Error fetching bulk post data for ln = " + ts.getLn() + " and id = " + ts.getId() + " and start = " 
							+ ts.getStart() + " and end = " + ts.getEnd());
					ts.setErrorMessage(e.getMessage());
				}
				ts.setErrorMessage(null);
			    return ts;
			} else {
				NodeStatus alternateLocation = dataHandler.getAlternateLocation(ts.getCluster(), ts.getLn());
				logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
				TimeSeries proxyTs = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).getTimeSeriesBulkJSON(ts);
				System.out.println("Executed proxy bulk post request for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
						+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
				logger.debug("Executed proxy bulk post request for ln = " + ts.getLn() + " id = " + ts.getId() + " start = " 
						+ ts.getStart() + " hfoffset = " + ts.getStartHfOffset() + " and end = " + ts.getEnd() + " and hfoffset = " + ts.getEndHfOffset());
				return proxyTs;
			}
		} catch (Exception e) {
			logger.error("Found error processing get request for _path /json/timeseries for ln = " 
					+ ts.getLn() + " and id = " + ts.getId() + " start = " + ts.getStart() + " end = " + ts.getEnd(), e);
			ts.setErrorMessage(e.getMessage());
		}
		return ts;
	}
	
	public TimeSeries putData(String cluster, int dataln, String guid, long start, int startoffset, long end, int endoffset, SortedMap<Long,Object> data) {
		TimeSeries ts = new TimeSeries(cluster, dataln, guid, start, startoffset, end, endoffset);
		try {
			if (dataHandler.getReplicaSet(cluster, dataln).contains(localLn)) {
				List<MasterData> mdList = dataHandler.getMasterData(cluster, dataln, guid, start, end);
				if (mdList == null || mdList.size() == 0 ) {
					logger.debug("No master data found for requested update");
					ts.setErrorMessage("No master data found for requested update");
					return ts;
				}
				if (data == null || data.size() == 0) {
					logger.debug("No time series specified for insert");
					ts.setErrorMessage("No time series specified for insert");
					return ts;
				}
				Schema.Type timeSeriesValueType = DynamicTypeTranslator.getSchemaType(mdList.get(0).getDatatype()); 
				AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
				handler.getSchema().resetSchema(timeSeriesValueType);
				GenericArray<GenericRecord> dataArray = new GenericData.Array<>(data.size(), handler.getSchema().getDataArraySchema());
				for (Long offset : data.keySet())
					dataArray.add(handler.setDataRecord(offset, data.get(offset)));
				
				GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
				blockArray.add(handler.setBlockRecord(cluster, dataln, guid, start, dataArray));
				GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
				final byte[] serializedBytes = handler.serialize(timeSeriesRecord);
				dataHandler.insertBulkData(serializedBytes);
			} else {
				NodeStatus alternateLocation = dataHandler.getAlternateLocation(cluster, dataln);
				logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
				//TimeSeries proxyTs = 
					ts = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).updateTimeSeriesJSON(ts);
				System.out.println("Executed proxy insert for ln = " + dataln + " id = " + guid + " start = " 
						+ start + " hfoffset = " + startoffset + " and end = " + end + " and hfoffset = " + endoffset);
				logger.debug("Executed proxy insert for ln = " + dataln + " id = " + guid + " start = " 
						+ start + " hfoffset = " + startoffset + " and end = " + end + " and hfoffset = " + endoffset);
			}
		} catch (TimeSeriesException | IOException e) {
			logger.error("Found error inserting time series for ln = " + dataln + " and id = " + guid + " and start = " + start + " and end = " + end);
		}
		return ts;
	}
	
	public TimeSeries putData(String cluster, int dataln, String guid, long start, int startoffset, long end, int endoffset, String encodedData) {
		TimeSeries ts = new TimeSeries(cluster, dataln, guid, start, startoffset, end, endoffset, encodedData);
		if (encodedData == null) {
			ts.setErrorMessage("Bulk data is null for request");
			return ts;
		}
		
		try {
			if (dataHandler.getReplicaSet(cluster, dataln).contains(localLn)) {

				byte[] serializedBytes = RESTFactory.decode(ts.getBulkData());
				dataHandler.insertBulkData(serializedBytes);
				System.out.println("Executed local insert for cluster + " + ts.getCluster() + " and bulk data size = " + serializedBytes.length);
				logger.debug("Executed local insert for cluster + " + ts.getCluster() + " and bulk data size = " + serializedBytes.length);
			} else {
				NodeStatus alternateLocation = dataHandler.getAlternateLocation(cluster, dataln);
				logger.debug("Found alternate location for data = " + alternateLocation.getAddress());
				//TimeSeries proxyTs = 
					ts = RESTFactory.getTimeSeriesProxy(alternateLocation.getAddress()).updateTimeSeriesBulkJSON(ts);
				System.out.println("Executed proxy insert for cluster + " + ts.getCluster() + " and bulk data = " + ts.getBulkData());
				logger.debug("Executed proxy insert for cluster + " + ts.getCluster() + " and bulk data = " + ts.getBulkData());
			}
		} catch (TimeSeriesException e) {
			logger.error("Found error inserting time series for ln = " + dataln + " and id = " + guid + " and start = " + start + " and end = " + end);
			ts.setErrorMessage(e.getMessage());
		}
		return ts;
	}
}
