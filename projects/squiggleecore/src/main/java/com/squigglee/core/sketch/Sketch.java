// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.utility.CollectionsUtility;

public abstract class Sketch implements ISketch {
	private static Logger logger = Logger.getLogger("com.squigglee.core.sketch.Sketch");
	protected long id = 0;
	protected int n = (int) Math.pow(2, 32);
	protected int scalar = 1;
	protected int topk = 0;
	protected long minKey = 0;
	protected long maxKey = 0;
	protected double max = Double.MIN_VALUE, min = Double.MAX_VALUE;
	protected double first, last;
	protected long count = 0L;
	protected Map<Object,Long> heavyHitters = new HashMap<Object,Long>();
	protected Map<Integer, long[][]> map = null;
	protected UniversalHashFunction[] hashers = null;

	protected void updateStats(long index, double val) {
		if (topk > 0)
			updateHeavyHitters(val);
		if (minKey == -1 || index < minKey)
			minKey = index;
		if (index > maxKey)
			maxKey = index;
		if (val >= max)
			max = val;
		if ( val <= min)
			min = val;
		if (count == 0)
			first = val;
		last = val;
		count++;
	}
	
	protected void reverseUpdateStats(long index, double val) {
		if (topk > 0)
			updateHeavyHitters(val);
		if (minKey == -1 || index < minKey)
			minKey = index;
		if (index > maxKey)
			maxKey = index;
		if (val >= max)
			max = val;
		if ( val <= min)
			min = val;
		if (count == 0)
			first = val;
		last = val;
		if (count > 0)
			count--;
	}
	
	protected void updateHeavyHitters(double val) {
		long freq = pointQuery(val);
		heavyHitters.put(new Double(val),freq);
		if (heavyHitters.size() > topk) {
			CollectionsUtility.removeMin(heavyHitters);
		}
	}
	
	@Override
	public Stats statistics() {
		Map<Long,Object> sorted = new TreeMap<Long,Object>(Collections.reverseOrder());
		for (Object o : heavyHitters.keySet())
			sorted.put(heavyHitters.get(o),o);
		Stats stats = new Stats(this.min, this.max, this.first, this.last, this.count, getTableName(), sorted);
		stats.setSketchDomainSize(this.n);
		return stats;
	}

	@Override
	public void updateIndex(String cluster, long id, IIndexHandler handler, boolean create) throws TimeSeriesException {
		String skchTbl = getTableName();
		byte[] serializedIndex = serializeIndex();
		System.out.println("Map contents prior to saving sketch");
		//printMap();
		handler.saveSerializedIndex(cluster, id, skchTbl, serializedIndex, create);
	}

	@Override
	public void loadSerializedIndex(String cluster, long id, String sketchIndexTbl, IIndexHandler handler) {
		byte[] serialized;
		boolean notFound = false;
		try {
			serialized = handler.loadSerializedIndex(cluster, id, sketchIndexTbl);
			if(serialized != null) {
				deserializeIndex(serialized);
				this.id = id;
				System.out.println("Map contents after deserialization");
				//printMap();
			}
			else
				notFound = true;
		} catch (TimeSeriesException e) {
			notFound = true;
		}
		if (notFound) {
			setValuesFromTableName(sketchIndexTbl + "_" + id);
			try {
				updateIndex(cluster, id, handler, true);
			} catch (TimeSeriesException e) {
				logger.error("Found error saving the newly created sketch: " + sketchIndexTbl + "_" + id);
			}
		}
	}
	
	protected void printMap() {
		System.out.println("Sketch map contents " + map);
		for (int t : map.keySet()) {
			long[][] table = map.get(t);
			for (int i = 0; i < table.length; i++) {
				for (int j = 0; j< table[i].length; j++)
					System.out.print(table[i][j] + ",");
				System.out.println();
			}
		}
	}
}
