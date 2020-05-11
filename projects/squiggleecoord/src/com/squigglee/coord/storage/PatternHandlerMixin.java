// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.storage;

import java.util.List;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.IPatternService;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.serializers.ITimeSeriesDeserializer;
import com.squigglee.core.serializers.ITimeSeriesSerializer;

public class PatternHandlerMixin extends IndexHandlerMixin {
	//private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.PatternIndexHandlerImpl");
	protected IPatternHandler handler = null;
	protected ITimeSeriesDeserializer deserializer = null;
	protected ITimeSeriesSerializer serializer = null;
	
	public PatternHandlerMixin(IPatternHandler handler, ITimeSeriesDeserializer deserializer, ITimeSeriesSerializer serializer) {
		this.handler = handler;
		this.deserializer = deserializer;
		this.serializer = serializer;
	}
	
	public int getNextMultiIndexId(String cluster, long id, String idxTableName) throws TimeSeriesException {
		IIndexService indexService = ServiceFactory.getIndexService();
		int ln = indexService.getLogicalNode(id);
		return indexService.getNextMultiIndexId(cluster, ln, id, idxTableName);
	}
	
	public void setNextMultiIndexId(String cluster, long id, String idxTableName, int currentId) throws TimeSeriesException {
		IIndexService indexService = ServiceFactory.getIndexService();
		int ln = indexService.getLogicalNode(id);
		indexService.setNextMultiIndexId(cluster, ln, id, idxTableName, currentId);
	}
	
	public boolean storePattern(String cluster, String pguid, List<String> pattern) throws TimeSeriesException {
		IPatternService ptrnService = ServiceFactory.getPatternService();
		ptrnService.storePattern(cluster, pguid, pattern);
		//ptrnService.close();
		return true;
	}
	
	public List<String> fetchPattern(String cluster, String pguid) throws TimeSeriesException {
		IPatternService ptrnService = ServiceFactory.getPatternService();
		return ptrnService.fetchPattern(cluster, pguid);
	}
	
	public List<String> fetchCapturedPatterns(String cluster) throws TimeSeriesException {
		IPatternService ptrnService = ServiceFactory.getPatternService();
		List<String> capturedPatterns = ptrnService.fetchCapturedPatterns(cluster);
		//ptrnService.close();
		return capturedPatterns;
	}
		
	public View getPatternView(View matchView) {
		View view = View.PATTERNDOUBLES;
		
		switch(matchView) {
		case MATCHEDBIGDECIMALS:
			break;
		case PATTERNBIGDECIMALS:
			break;
		case BIGDECIMALS:
			break;
		case MATCHEDBIGINTEGERS:
			break;
		case PATTERNBIGINTEGERS:
			break;
		case BIGINTEGERS:
			break;
		case MATCHEDBLOBS:
			break;
		case PATTERNBLOBS:
			break;
		case BLOBS:
			break;
		case MATCHEDBOOLEANS:
			break;
		case PATTERNBOOLEANS:
			break;
		case BOOLEANS:
			break;
		case BULKDATA:
			break;
		case MATCHEDCLOBS:
			break;
		case PATTERNCLOBS:
			break;
		case CLOBS:
			break;
		case MATCHEDDOUBLES:
			view = View.PATTERNDOUBLES;
			break;
		case PATTERNDOUBLES:
			break;
		case DOUBLES:
			break;
		case MATCHEDFLOATS:
			break;
		case PATTERNFLOATS:
			break;
		case FLOATS:
			break;
		case MATCHEDINTEGERS:
			view = View.PATTERNINTEGERS;
			break;
		case PATTERNINTEGERS:
			break;
		case INTEGERS:
			break;
		case MATCHEDLONGS:
			break;
		case PATTERNLONGS:
			break;
		case LONGS:
			break;
		case MASTERDATA:
			break;
		case SAMPLEDBIGDECIMALS:
			break;
		case SAMPLEDBIGINTEGERS:
			break;
		case SAMPLEDBLOBS:
			break;
		case SAMPLEDBOOLEANS:
			break;
		case SAMPLEDCLOBS:
			break;
		case SAMPLEDDOUBLES:
			break;
		case SAMPLEDFLOATS:
			break;
		case SAMPLEDINTEGERS:
			break;
		case SAMPLEDLONGS:
			break;
		case SAMPLEDSTRINGS:
			break;
		case SKETCHEDBIGDECIMALS:
			break;
		case SKETCHEDBIGINTEGERS:
			break;
		case SKETCHEDBLOBS:
			break;
		case SKETCHEDBOOLEANS:
			break;
		case SKETCHEDCLOBS:
			break;
		case SKETCHEDDOUBLES:
			break;
		case SKETCHEDFLOATS:
			break;
		case SKETCHEDINTEGERS:
			break;
		case SKETCHEDLONGS:
			break;
		case SKETCHEDSTRINGS:
			break;
		case MATCHEDSTRINGS:
			break;
		case PATTERNSTRINGS:
			break;
		case STRINGS:
			break;
		default:
			break;
		}
		return view;
	}

}
