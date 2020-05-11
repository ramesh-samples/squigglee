// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

import com.squigglee.core.entity.IndexType;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.sketch.CountExact;
import com.squigglee.core.sketch.CountMin;
import com.squigglee.core.sketch.ISketch;

public class SketchHandlerMixin extends IndexHandlerMixin {
	//private static Logger logger = Logger.getLogger("com.squigglee.coord.storage.SketchHandlerMixin");
	protected IIndexHandler handler = null;
	public SketchHandlerMixin(IIndexHandler handler) {
		this.handler = handler;
	}
	
	public Stats statistics(String cluster, int ln, String guid)
			throws TimeSeriesException {
		double globalmin = Double.MAX_VALUE;
		double globalmax = Double.MIN_VALUE;
		long domainSizeMax = Long.MIN_VALUE;
		double globalStart = 0.0, globalEnd = 0.0;
		long globalcount = 0;
		Map<Object,Long> globalHH = new HashMap<Object,Long>();
		
		List<TimeSeriesConfig> clist = getMasterData(cluster, ln, guid);
		boolean sketched = false;
		DateTime sketchEndTime = null;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(cluster, ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : new IndexType[]{IndexType.skchCM, IndexType.skchEX}) {
					ISketch sketch = null;
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					if (sketchConfigs == null || sketchConfigs.isEmpty())
						continue;
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getCluster(), md.getId(), sketchConfig, handler);
						Stats stats = sketch.statistics();
						sketched = true;
	 					if (stats.getMin() < globalmin)
	 						globalmin = stats.getMin();
	 					if (stats.getMax() > globalmax)
	 						globalmax = stats.getMax();
	 					if (sketchEndTime == null) {
	 						globalStart = stats.getFirst();
	 					}
	 					globalEnd = stats.getLast();
	 					sketchEndTime = config.getEndDate();
	 					if (stats.getSketchDomainSize() > domainSizeMax)
	 						domainSizeMax = stats.getSketchDomainSize();
	 					globalcount += stats.getCount();
	 					for (Long f : stats.getHeavyHitters().keySet()) {
	 						if (globalHH.containsKey(stats.getHeavyHitters().get(f)))
	 							globalHH.put(stats.getHeavyHitters().get(f),globalHH.get(stats.getHeavyHitters().get(f)) + f);
	 						else
	 							globalHH.put(stats.getHeavyHitters().get(f), f);
	 						//CollectionsUtility.removeMin(globalHH);
	 					}
					}
				}
			}
		}
		Map<Long,Object> sorted = new TreeMap<Long,Object>(Collections.reverseOrder());
		for (Object o : globalHH.keySet())
			sorted.put(globalHH.get(o),o);
		if (sketched) {
			Stats globalstats = new Stats(globalmin, globalmax, globalStart, globalEnd, globalcount, "Global Stats for node " + ln + " and parameter " + guid, sorted);
			globalstats.setSketchDomainSize(domainSizeMax);
			return globalstats;
		}
		else
			return null;
	}
	
	public long pointQuery(String cluster, int ln, String guid, Object val)
			throws TimeSeriesException {
		
		List<TimeSeriesConfig> clist = getMasterData(cluster, ln, guid);
		long pointFreq = 0L;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(cluster, ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : IndexType.values()) {
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					ISketch sketch = null;
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getCluster(), md.getId(), sketchConfig, handler);
						pointFreq += sketch.pointQuery(Double.parseDouble(val.toString()));
					}
				}
			}
		}
		return pointFreq;
	}
	
	public long rangeQuery(String cluster, int ln, String guid, Object startVal,
			Object endVal) throws TimeSeriesException {
		List<TimeSeriesConfig> clist = getMasterData(cluster, ln, guid);
		long rangeFreq = 0L;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(cluster, ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : IndexType.values()) {
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					ISketch sketch = null;
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getCluster(), md.getId(), sketchConfig, handler);
						//pointFreq += cm.pointQuery(Double.parseDouble(val.toString()));
						rangeFreq += sketch.rangeQuery(Double.parseDouble(startVal.toString()),Double.parseDouble(endVal.toString()));
					}
				}
			}
		}
		return rangeFreq;
	}
	
	public ISketch getSketch(String cluster, int ln, String guid) throws TimeSeriesException {
		List<TimeSeriesConfig> clist = getMasterData(cluster, ln, guid);
		ISketch sketch = null;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(cluster, ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : IndexType.values()) {
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getCluster(), md.getId(), sketchConfig, handler);
						break;
					}
				}
			}
		}
		return sketch;
	}
	
	public SortedMap<Integer,Long> getSketchHistogram(String cluster, int ln, String guid, int bins)  throws TimeSeriesException {
		SortedMap<Integer,Long> map = new TreeMap<Integer,Long>();
		List<TimeSeriesConfig> clist = getMasterData(cluster, ln, guid);
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(cluster, ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : IndexType.values()) {
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					ISketch sketch = null;
					if (sketchConfigs == null)
						continue;
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getCluster(), md.getId(), sketchConfig, handler);
						Stats stats = sketch.statistics();
						double interval = Math.floor((stats.getSketchDomainSize())*1.0/bins);
						for (int i = 0; i< bins; i++) {
							double intervalStart = i*interval + 1;
							double intervalEnd = (i+1)*interval;
							long rangeResult = sketch.rangeQuery(intervalStart, intervalEnd);
							if (i == (bins - 1))
								intervalEnd = stats.getSketchDomainSize();
							if (!map.containsKey(i))
								map.put(i, rangeResult);
							else
								map.put(i, map.get(i) + rangeResult);
						}
					}
				}
			}
		}
		
		return map;
	}

	public Object inverseQuery(String cluster, int ln, String guid, double quantile) throws TimeSeriesException {
		Stats stats = statistics(cluster, ln, guid);
		if (quantile <= 0.0)
			return 1;
		if (quantile >= 1.0)
			return stats.getSketchDomainSize();
		
		double tolerance = 0.001;
		int maxIter = 50;
		int iter = 0;
		double left = 1;
		double right = stats.getSketchDomainSize();
		double mid = 0;
		//boolean movedLeft = false;
		ISketch sketch = getSketch(cluster, ln, guid);
		if (sketch == null)
			return null;
		while (iter++ < maxIter) {
			mid = Math.round((right+left)/2.0);
			//if (movedLeft)
			//	mid = Math.ceil((right-left)/2.0);
			//else
			//	mid = Math.floor((right-left)/2.0);
			if (mid < 1)
				mid = 1;
			if (mid > stats.getSketchDomainSize())
				mid = stats.getSketchDomainSize();
			double lr = sketch.rangeQuery(1, mid)*1.0/stats.getCount(); 
			if ( Math.abs(lr - quantile) <= tolerance || Math.abs(right - left) <= 1) {
				//System.out.println("Convergence Achieved in " + iter + " steps");
				break;
			}
			else {
				if (lr > quantile) {	//move left
					right = mid;
					//movedLeft = true;
				} else {				// move right
					left = mid;
					//movedLeft = false;
				}
			}
		}
		return new Integer((int) Math.round(mid));
		//throw new TimeSeriesException("Quantile query failed to converge within " + maxIter + " iterations for quantile = " + quantile + " for sketch id = " + guid);
	}

}
