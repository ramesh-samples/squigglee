// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.core.algorithm.RandomSampler;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISketchHandler;

public class ISampledDataHandlerImpl extends IDataHandlerImpl implements ISampledDataHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.ISampledDataHandlerImpl");

	@Override
	public TimeSeries readBlockData(String cluster, int ln, String guid, long begints, int beginHfOffset, long endts, int endHfOffset, int sampleSize) throws TimeSeriesException {
		TimeSeries ts = new TimeSeries(cluster, ln, guid, begints, endts);
		
		List<MasterData> mdList = getMasterDataForBlocks(cluster, ln, guid, begints, endts);
		
		//TODO currently only supporting random sampling of a single time series 
		long[] dataRange = this.getDataStatus(mdList.get(0));
		long range = dataRange[1] - dataRange[0] + 1;
		//long range = endts - begints + 1;

		//if (mdList.get(0).getFreq().equals(Frequency.MICROS)) {
		//	range = (endts*1000 + endHfOffset) - (begints*1000 + beginHfOffset);
		//} else if (mdList.get(0).getFreq().equals(Frequency.MICROS)) {
		//	range = (endts*1000000 + endHfOffset) - (begints*1000000 + beginHfOffset);
		//}
		
		List<Long> sample = (new RandomSampler()).sampleWithoutReplacementS(sampleSize, range);
		//System.out.println(sample);
		Map<Long, File> fileMap = new HashMap<Long,File>();
		for (MasterData md : mdList)
			fileMap.put(md.getId(), new File(storagePath + "/data" + "_" + md.getId()));
		long offsetCount = TimeSeriesShard.getOffsetCount(mdList.get(0));
		SortedMap<Long,Object> result = new TreeMap<Long,Object>();
		try {
			for (long s : sample) {
				for (MasterData md : mdList) {
					
					long startOffset = TimeSeriesShard.getOffset(md.getFreq(), md.getStartts());
					if (s >= startOffset && s <= (startOffset + offsetCount) ) {
						long offset = s - startOffset;
						if (md.getFreq().equals(Frequency.MICROS)) {
							offset = s - startOffset*1000;
						} else if (md.getFreq().equals(Frequency.MICROS)) {
							offset = s - startOffset*1000000;
						}
						result.putAll(readBufferData(md, offset, offset));
						break;
					}
				}
			}
		} catch (IOException e) {
			logger.error("Found error reading block data for cluster " + cluster + " node " + ln + " id " + guid + " and time range [" + begints + "," + endts + "]", e);
			ts.setErrorMessage(e.getMessage());
		}
		ts.setData(result);
		return ts;
	}
	
	@Override
	public SortedMap<Integer, Long> getSampledDataHistogram(String cluster, int ln, String guid, int bins, long begints, int beginHfOffset, long endts, int endHfOffset, 
			int sampleSize, ISketchHandler sketchHandler)  throws TimeSeriesException {
		if (sampleSize > 10000)
			logger.debug("Large sample size of " + sampleSize + " requested for node " + ln + " and parameter " + guid);
		Stats stats = sketchHandler.statistics(cluster, ln, guid);
		double binDataInterval = (long) Math.floor(stats.getSketchDomainSize()*1.0/bins);
		SortedMap<Integer,Long> sampledHistogram = new TreeMap<Integer,Long>();
		for ( int i=0; i< bins; i++)
				sampledHistogram.put(new Integer(i), new Long(0));
		TimeSeries ts = readBlockData(cluster, ln, guid, begints, beginHfOffset, endts, endHfOffset, sampleSize);
		System.out.println("Time Series Data = " + ts.getData());
		for (long l : ts.getData().keySet()) {	
			int binnum = (int) Math.floor( (Double.parseDouble(ts.getData().get(l).toString()) - 1.0) / binDataInterval);
			if (sampledHistogram.containsKey(binnum))
				sampledHistogram.put(binnum, (sampledHistogram.get(binnum).longValue() + 1L) );
			else
				sampledHistogram.put(binnum,1L);
		}
		return sampledHistogram;
	}
}
