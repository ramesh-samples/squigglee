// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.vdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.avro.Schema;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.mapdb.DB;

import com.squigglee.coord.interfaces.IPattern;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesConfig;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.IndexType;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.interfaces.View;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.sketch.LocalitySensitiveHasher;
import com.squigglee.core.sketch.Match;

public class PatternHandlerImpl extends IndexHandlerImpl implements IPatternHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.vdb.PatternIndexHandlerImpl");
	
	public PatternHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public List<Long> fetchCandidatePatterns(MasterData md, String idxTableName, List<String> hashes)
			throws TimeSeriesException {
		List<Long> candidates = new ArrayList<Long>();
		
		MVStore s = getStore(idxTableName + "_" + md.getId());
		MVMap<Object, Object> map = getMap(s, IndexType.parseIndexTableName(idxTableName).toString());
		
		//DB db = getDatabase(idxTableName + "_" + id);
		//ConcurrentNavigableMap<Object, Object> map = getTable(db, IndexType.parseIndexTableName(idxTableName).toString());
		
		Object o = null;
		for (String hash : hashes) {
			try {
				if (hash == null)
					continue;
				o = map.get(hash);
			} catch (Exception ex) {
				logger.error("Found error querying pattern table for hash = " + hash + " for pattern index = " + idxTableName + "_" + md.getId());
				ex.printStackTrace();
			}
			if (o != null) {
				String list = o.toString().replaceAll("\\s","").replaceAll("\\[", "").replaceAll("\\]", "");
				for (String l : list.split(",")) {					
					long val = Long.parseLong(l);
					if (!candidates.contains(val))
						candidates.add(val);
				}
			}
		}
		//s.close();
		//db.close();
		return candidates;
	}

	@Override
	public Map<Long, Match> fetchCandidateTimeSeries(MasterData md, List<Long> candidates, int size)
			throws TimeSeriesException {
		SortedMap<Long,Match> output = new TreeMap<Long,Match>();
		
		double[] matchedTs = new double[size];
		int[] offsets = new int[size];
		int cntr = 0;
		
		MVStore s = getStore("" + md.getId());
		MVMap<Object, Object> table = getMap(s, md.getDatatype());
		
		//DB db = getDatabase("" + id);
		//ConcurrentNavigableMap<Object, Object> map = getTable(db, dataType);
		for (long c : candidates) {
			Iterator it = table.keyIterator(c);
			while(it.hasNext()) {
				Long l = Long.parseLong(it.next().toString());
				if (cntr == size)
					break;
				matchedTs[cntr] = Double.parseDouble(table.get(l).toString());
				offsets[cntr] = l.intValue();
			//for (Object o : map.subMap(c, (c + size)).keySet()) {
				//matchedTs[cntr] = Double.parseDouble(map.get(o).toString());
				//offsets[cntr] = Integer.parseInt(o.toString());
				cntr++;
			}
			if (cntr == size)
				output.put(c,new Match(md.getId(), offsets, matchedTs));
		}
		//s.close();
		//db.close();
		return output;
	}

	@Override
	public byte[] fetchMatches(List<TimeSeriesConfig> list,
			String pguid, View view, int topk, double radius)
			throws TimeSeriesException {
		
		List<Object> pvals = fetchPattern(pguid, getPatternView(view));
		double[] pvalarr = new double[pvals.size()];
		for (int i=0; i< pvalarr.length; i++)
			pvalarr[i] = Double.parseDouble(pvals.get(i).toString());
		double[] normPattern = StatUtils.normalize(pvalarr);
		return fetchMatches(list, normPattern, topk, radius);
	}
	
	@Override
	public byte[] fetchMatches(List<TimeSeriesConfig> list,	double[] pattern_norm, int topk, double radius)
			throws TimeSeriesException {
		List<MasterData> mdList = new ArrayList<MasterData>();
		for (TimeSeriesConfig tc : list)
			mdList.addAll(getMasterData(tc.getLogicalNode(), tc.getGuid(),tc.getStartDate().getMillis(),tc.getEndDate().getMillis()));
		SortedMap<Double,List<Match>> matches = new TreeMap<Double,List<Match>>();
		
		for (MasterData md : mdList) {
			LocalitySensitiveHasher lsh = new LocalitySensitiveHasher();
			if (md.getIndexes() == null || !md.getIndexes().toLowerCase().contains("ptrn"))
				continue;
			String ptrnIndex = null;  
			String[] indices = md.getIndexes().split(";");
			for (String idx : indices) {
				if (idx.toLowerCase().contains("ptrn")) {
					ptrnIndex = idx;
					break;		// currently supporting only one pattern index at a time
				}
			}
			if (ptrnIndex == null)
				throw new TimeSeriesException("No pattern indexes are available for node " + md.getLn() + " and parameter " + md.getGuid());
			lsh.loadSerializedIndex(md.getId(), ptrnIndex, this);	 
			List<Long> candidates = lsh.getStoredIndexedNeighbors(pattern_norm, md.getId(), md.getDatatype(), this);
			logger.debug("Found " + candidates.size() + " potential match candidates for node " + md.getLn() + " and parameter " 
					+ md.getGuid() + " and topk = " + topk + " and radius = " + radius);
			System.out.println("Found " + candidates.size() + " potential match candidates for node " + md.getLn() + " and parameter " 
					+ md.getGuid() + " and topk = " + topk + " and radius = " + radius);
			if (candidates.size() > LocalNodeProperties.getMaxMatchCandidates()) {
				logger.debug("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
				System.out.println("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
				candidates = candidates.subList(0, (LocalNodeProperties.getMaxMatchCandidates() - 1));
			}
			matches = lsh.getStoredExactMatches(pattern_norm, candidates, topk, radius, md.getId(), md.getDatatype(), this, matches);
		}
		
		if (matches == null || matches.size() == 0) {
			logger.debug("Found no " + " matches within search radius " + radius + ";");
			return null;
		}

		List<Match> topkMatches = getTopkMatches(matches,topk);
		
		if (topkMatches.size() == 0) {
			return null;
		}

		serializer.reset();
		Schema.Type dataType = DynamicTypeTranslator.getSchemaType(mdList.get(0).getDatatype());
		serializer.resetSchema(dataType) ;
		serializer.setBlockCount(matches.size());
		for (Match match : topkMatches) {	
			MasterData md = null;
			for ( MasterData mdata : mdList) {
				if (match.getId() == mdata.getId()) {
					md = mdata;
					break;
				}
			}
			serializer.startNewBlock(md.getLn(), md.getGuid(), md.getStartts(), match.getSize());
			for (int i =0; i< match.getSize(); i++) {
				//Object dataVal = DynamicTypeTranslator.parseStringObject( (new Double(match.getValues()[i])).toString(), serializer.getDataType());
				Object dataVal = DynamicTypeTranslator.convertDoubleToNumeric(match.getValues()[i] , md.getDatatype());
				serializer.setData(match.getOffsets()[i], dataVal);
			}
		}
		return serializer.getRawData();
	}
	
	private List<Match> getTopkMatches(SortedMap<Double,List<Match>> matches, int topk) {
		List<Match> topkMatches = new ArrayList<Match>();
		for (double dist : matches.keySet()) {
			for (Match match : matches.get(dist)) {
				if (topkMatches.size() < topk)
					topkMatches.add(match);
				else
					return topkMatches;
			}
		}
		return topkMatches;
	}

	@Override
	public void updatePatternIndex(MasterData md, String idxTableName, long startKey, long endKey,
			Map<String, List<Long>> lookup, int projHashCount, int projHashNumber) {

		if (!getConfigurationStatus(md.getId(),idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			return;
		}
		
		MVStore s = getStore(idxTableName + "_" + md.getId());
		MVMap<Object, Object> map = getMap(s, IndexType.parseIndexTableName(idxTableName).toString());
		
		//DB db = getDatabase(idxTableName + "_" + id);
		//ConcurrentNavigableMap<Object,Object> map =  getTable(db, IndexType.parseIndexTableName(idxTableName).toString());
		
		//int retryCounter = 0;
		//int retries = 2;
		//int commitCounter = 0;
		Object o = null;
		for (String hash : lookup.keySet()) {
			//while (retryCounter <= retries) {
			//	try {
					o = lookup.get(hash);
					//if (o == null)
					//	continue;
					map.put(hash, o.toString());
			//		break;
			//	} catch (Exception ex) {
			//		retryCounter++;
			//		System.out.println("hash = " + hash + " and value = " + o + " at retry value = " + retryCounter);
			//		ex.printStackTrace();
			//		commit(idxTableName + "_" + id);
			//	}
			//}
		}
		s.commit();
		//s.close();
		//db.commit();
		//db.close();
	}

	@Override
	public boolean storePattern(String pguid, List<Object> pattern, View view) throws TimeSeriesException {
		IPattern ptrnService = serviceFactory.getPatternService();
		ptrnService.storePattern(this.clusterName, pguid, pattern, view);
		ptrnService.close();
		return true;
	}

	@Override
	public List<Object> fetchPattern(String pguid, View view) throws TimeSeriesException {
		IPattern ptrnService = serviceFactory.getPatternService();
		List<Object> ptrn = ptrnService.fetchPattern(this.clusterName, pguid, view);
		ptrnService.close();
		return ptrn;
	}
	
	@Override
	public List<String> fetchCapturedPatterns(View view) throws TimeSeriesException {
		IPattern ptrnService = serviceFactory.getPatternService();
		List<String> capturedPatterns = ptrnService.fetchCapturedPatterns(this.clusterName, view);
		ptrnService.close();
		return capturedPatterns;
	}
		
	protected View getPatternView(View matchView) {
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

	@Override
	public boolean createPatternTable(View view) throws TimeSeriesException {
		// TODO Auto-generated method stub
		return false;
	}
}
