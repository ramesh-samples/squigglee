// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.util.SortedMap;
import com.squigglee.coord.storage.SketchHandlerMixin;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.interfaces.Stats;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.interfaces.View;
import com.squigglee.core.sketch.ISketch;

public class SketchHandlerImpl extends IndexHandlerImpl implements ISketchHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.SketchHandlerImpl");
	protected SketchHandlerMixin shMixin = null;
	public SketchHandlerImpl() {
		super();
		shMixin = new SketchHandlerMixin(this.clusterName, this.ln, this);
	}
	@Override
	public Stats statistics(int ln, String guid, View view)	throws TimeSeriesException {
		return shMixin.statistics(ln, guid, view);
		
		/*
		double globalmin = Double.MAX_VALUE;
		double globalmax = Double.MIN_VALUE;
		long domainSizeMax = Long.MIN_VALUE;
		double globalStart = 0.0, globalEnd = 0.0;
		long globalcount = 0;
		Map<Object,Long> globalHH = new HashMap<Object,Long>();
		
		List<TimeSeriesConfig> clist = getMasterData(ln, guid);
		boolean sketched = false;
		DateTime sketchEndTime = null;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
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
						sketch.loadSerializedIndex(md.getId(), sketchConfig, this);
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
			*/
	}
	
	@Override
	public long pointQuery(int ln, String guid, View view, Object val)	throws TimeSeriesException {
		return shMixin.pointQuery(ln, guid, view, val);
		
		/*
		List<TimeSeriesConfig> clist = getMasterData(ln, guid);
		long pointFreq = 0L;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
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
						sketch.loadSerializedIndex(md.getId(), sketchConfig, this);
						pointFreq += sketch.pointQuery(Double.parseDouble(val.toString()));
					}
				}
			}
		}
		return pointFreq;
		*/
	}
	
	@Override
	public long rangeQuery(int ln, String guid, View view, Object startVal,	Object endVal) throws TimeSeriesException {
		return shMixin.rangeQuery(ln, guid, view, startVal, endVal);
		
		/*
		List<TimeSeriesConfig> clist = getMasterData(ln, guid);
		long rangeFreq = 0L;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
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
						sketch.loadSerializedIndex(md.getId(), sketchConfig, this);
						//pointFreq += cm.pointQuery(Double.parseDouble(val.toString()));
						rangeFreq += sketch.rangeQuery(Double.parseDouble(startVal.toString()),Double.parseDouble(endVal.toString()));
					}
				}
			}
		}
		return rangeFreq;
		*/
	}
	
	public ISketch getSketch(int ln, String guid, View view) throws TimeSeriesException {
		return shMixin.getSketch(ln, guid, view);
		
		/*
		List<TimeSeriesConfig> clist = getMasterData(ln, guid);
		ISketch sketch = null;
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
				for (IndexType it : IndexType.values()) {
					List<String> sketchConfigs = parseIndexString(it, md.getIndexes());
					for (String sketchConfig : sketchConfigs) {
						if (it.equals(IndexType.skchCM))
							sketch = new CountMin();
						else if (it.equals(IndexType.skchEX))
							sketch = new CountExact();
						else
							continue;
						sketch.loadSerializedIndex(md.getId(), sketchConfig, this);
						break;
					}
				}
			}
		}
		return sketch;
		*/
	}
	
	@Override
	public SortedMap<Integer,Long> getSketchHistogram(int ln, String guid, View view, int bins)  throws TimeSeriesException {
		return shMixin.getSketchHistogram(ln, guid, view, bins);
		/*
		SortedMap<Integer,Long> map = new TreeMap<Integer,Long>();
		List<TimeSeriesConfig> clist = getMasterData(ln, guid);
		for (TimeSeriesConfig config : clist) {
			for (MasterData md : getMasterData(ln, guid, config.getStartDate().getMillis(), config.getEndDate().getMillis())) {
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
						sketch.loadSerializedIndex(md.getId(), sketchConfig, this);
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
		*/
	}

	@Override
	public Object inverseQuery(int ln, String guid, View view, double quantile) throws TimeSeriesException {
		return shMixin.inverseQuery(ln, guid, view, quantile);
		
		/*
		Stats stats = statistics(ln, guid, view);
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
		ISketch sketch = getSketch(ln, guid, view);
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
		*/
	}

}
