// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.mbb;

import java.util.SortedMap;

import com.squigglee.coord.storage.SketchHandlerMixin;
import com.squigglee.core.entity.Stats;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.sketch.ISketch;

public class ISketchHandlerImpl extends IIndexHandlerImpl implements ISketchHandler {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.mbb.ISketchHandlerImpl");
	protected SketchHandlerMixin shMixin = null;
	
	@Override
	public void initialize() {
		super.initialize();
		shMixin = new SketchHandlerMixin(this);
	}
	
	@Override
	public Stats statistics(String cluster, int ln, String guid)	throws TimeSeriesException {
		return shMixin.statistics(cluster, ln, guid);
	}
	
	@Override
	public long pointQuery(String cluster, int ln, String guid, Object val)	throws TimeSeriesException {
		return shMixin.pointQuery(cluster, ln, guid, val);
	}
	
	@Override
	public long rangeQuery(String cluster, int ln, String guid, Object startVal,	Object endVal) throws TimeSeriesException {
		return shMixin.rangeQuery(cluster, ln, guid, startVal, endVal);
	}
	
	public ISketch getSketch(String cluster, int ln, String guid) throws TimeSeriesException {
		return shMixin.getSketch(cluster, ln, guid);
	}
	
	@Override
	public SortedMap<Integer,Long> getSketchHistogram(String cluster, int ln, String guid, int bins)  throws TimeSeriesException {
		return shMixin.getSketchHistogram(cluster, ln, guid, bins);
	}

	@Override
	public Object inverseQuery(String cluster, int ln, String guid, double quantile) throws TimeSeriesException {
		return shMixin.inverseQuery(cluster, ln, guid, quantile);
	}

}
