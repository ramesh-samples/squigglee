// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mapdb;

import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.mapdb.DB;

import com.squigglee.core.algorithm.RandomSampler;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.interfaces.Stats;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.interfaces.View;
import com.squigglee.core.serializers.avro.AvroTimeSeriesDeserializer;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;

public class SampledDataHandlerImpl extends MasterDataHandlerImpl implements ISampledDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mapdb.SampledDataHandlerImpl");

	@Override
	public byte[] readBlockData(int ln, String guid, long begints, long endts, long sampleSize, long dataSize) throws TimeSeriesException {
		List<Long> sample = (new RandomSampler()).sampleWithoutReplacementS(sampleSize, dataSize);
		
		int chunkSize = 10000;
		int numOfChunks = (int) (Math.ceil(sample.size() * 1.0 / chunkSize));
		
		serializer.reset();
		List<MasterData> mdList = getMasterDataForBlocks(ln, guid, begints, endts);	
		serializer.resetSchema(DynamicTypeTranslator.getSchemaType(mdList.get(0).getDatatype())) ;
		serializer.setBlockCount(mdList.size());
		MasterData md = null;
		StringBuilder builder = new StringBuilder();
		builder.append("(");
		for (int i = 0; i < mdList.size(); i++ ) {
			md = mdList.get(i);
			long initialOffset = begints - md.getStartts();
			//Keyspace dataKeyspace = getKeyspace(md.getKs());
			//List<TimeSeriesColumn> chunkSample = new ArrayList<TimeSeriesColumn>();
			for (int n = 0 ; n < numOfChunks; n++) {
				long fromIndex = n*chunkSize;
				long toIndex = (n+1)*chunkSize;
				if (toIndex > sample.size() )
					toIndex = sample.size();
				
				SortedMap<Object,Object> fetched = null;
				
				DB db = getDatabase("" + md.getId());
				fetched = getTable("" + md.getId(), md.getDatatype());
				
				fetched = fetched.subMap(new Long(fromIndex), new Long(toIndex));
				for (int j = 0; j< sample.size(); j++) {
					
					//fetched = fetchTimeSeries(1L, "int", fromIndex, 0, toIndex, 0);
				}
				if (fetched == null || fetched.isEmpty())
					continue;
				if (begints > md.getStartts())
					serializer.startNewBlock(md.getLn(), md.getGuid(), begints, (int) (toIndex - fromIndex + 1));
				else
					serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), (int) (toIndex - fromIndex + 1));
				for (Object l : fetched.keySet())
					serializer.setData( (Long) l - initialOffset, fetched.get(l));
				
				//s.close();
				//db.close();
			}
		}
		return serializer.getRawData();
	}
	
	@Override
	public SortedMap<Integer, Long> getSampledDataHistogram(int ln, String guid, View view, int bins, long begints, long endts,
			long sampleSize, String dataType, ISketchHandler sketchHandler)  throws TimeSeriesException {
		if (sampleSize > 10000)
			logger.debug("Large sample size of " + sampleSize + " requested for node " + ln + " and parameter " + guid);
		Stats stats = sketchHandler.statistics(ln, guid, view);
		double binDataInterval = (long) Math.floor(stats.getSketchDomainSize()*1.0/bins);
		SortedMap<Integer,Long> sampledHistogram = new TreeMap<Integer,Long>();
		for ( int i=0; i< bins; i++)
				sampledHistogram.put(new Integer(i), new Long(0));
		byte[] sampledData = readBlockData(ln, guid, begints, endts, sampleSize, stats.getCount());
		
		AvroTimeSeriesDeserializer atsd = new AvroTimeSeriesDeserializer();
		atsd.resetSchema(DynamicTypeTranslator.getSchemaType(dataType));
		atsd.setRawData(sampledData);
		int numOfBlocks = atsd.getBlockCount();
		for (int i=0; i< numOfBlocks; i++) {
			int dataCount = atsd.getDataCount(i);
			for (int j=0; j< dataCount; j++) {	
				int binnum = (int) Math.floor( (Double.parseDouble(atsd.getVal(i, j).toString()) - 1.0) / binDataInterval);
				if (sampledHistogram.containsKey(binnum))
					sampledHistogram.put(binnum, (sampledHistogram.get(binnum).longValue() + 1L) );
				else
					sampledHistogram.put(binnum,1L);
			}
		}
		return sampledHistogram;
	}
}
