// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.squigglee.coord.interfaces.IPattern;
import com.squigglee.coord.utility.ServiceFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesConfig;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.interfaces.View;
import com.squigglee.core.serializers.avro.DynamicTypeTranslator;
import com.squigglee.core.sketch.LocalitySensitiveHasher;
import com.squigglee.core.sketch.Match;

public class PatternHandlerImpl extends IndexHandlerImpl implements IPatternHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.cassandra.PatternIndexHandlerImpl");
	//private PreparedStatement psInsertPattern = null;
	
	@Override
	public List<Long> fetchCandidatePatterns(MasterData md, String idxTableName, List<Integer> hashes)
			throws TimeSeriesException {
		List<Long> candidates = new ArrayList<Long>();
		for (int hash : hashes) {
			//String[] tokens = hash.split("#"); // first k are hashes, last is the projection 
			//String select = "select id, val from " + idxTableName + "_" + md.getId()  + "_" + tokens[tokens.length-1] + " where id = " + md.getId();
			//for (int i=0; i< tokens.length-1; i++)
			//	select += " and k" + (i+1) + " = " + tokens[i];
			//select += ";";
			//ResultSet rs = getSession(md.getKs()).execute(select);
			//Iterator<Row> it = rs.iterator();
			//while (it.hasNext()) {		
			//	Row row = it.next();
			//	if (!candidates.contains((long) row.getInt("val")))
			//		candidates.add((long) row.getInt("val"));
			//}
		}
		return candidates;
	}

	@Override
	public Map<Long, Match> fetchCandidateTimeSeries(MasterData md, List<Long> candidates, int size)
			throws TimeSeriesException {
		SortedMap<Long,Match> output = new TreeMap<Long,Match>();
		for (long c : candidates) {
			SortedMap<Long, Object> fetchedTs = fetchTimeSeriesLimit(md,c, (long) TsrConstants.COLUMN_FAMILY_MAX_COLUMNS, size,  false);
			output.put(c,new Match(md.getId(), fetchedTs));
		}
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
			LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
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
			List<Long> candidates = lsh.getStoredIndexedNeighbors(pattern_norm, md.getId(), md.getDatatype(),  this);
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
			for (long key : match.getValues().keySet()) {
				//Object dataVal = DynamicTypeTranslator.parseStringObject( (new Double(match.getValues()[i])).toString(), serializer.getDataType());
				//Object dataVal = DynamicTypeTranslator.convertDoubleToNumeric(match.getValues()[i] , md.getDatatype());
				serializer.setData(key, match.getValues().get(key));
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
			Map<Integer, List<Long>> lookup, int projHashCount, int projHashNumber) {
		
		BatchStatement bs = new BatchStatement();
		int indexNum = 10000;
		int indexCnt = 0;
		for (int s : lookup.keySet()) {
			for ( long val : lookup.get(s)) {
				if (++indexCnt == 1)
					bs.clear();
				//String[] tokens = s.split("#");	//first k hashes and the last is the projection 
				//String insert = "insert into " + idxTableName + "_" + md.getId()  + "_" + tokens[tokens.length-1] + " (id";
				//for (int i=0; i< (tokens.length-1); i++)
				//	insert += ",k" + (i+1);
				//insert += ",val,dummy) values (" + md.getId();
				//for (int i=0; i< (tokens.length-1); i++)
				//	insert += ", " + tokens[i];
				//insert += ", " + val + ", null);";
				//bs.add(new SimpleStatement(insert));
				if (indexCnt == indexNum) {
					getSession(md.getKs()).execute(bs);
					indexCnt = 0;
				}
			}
			//System.out.println("Completed Incremental Update for " + s);
		}
		if (bs.getStatements().size() > 0)
			getSession(md.getKs()).execute(bs);

		if (!getConfigurationStatus(md.getId(),idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			return;
		}
	}

	@Override
	public boolean storePattern(String pguid, List<Object> pattern, View view) throws TimeSeriesException {
		IPattern ptrnService = ServiceFactory.getPatternService();
		ptrnService.storePattern(this.clusterName, pguid, pattern, view);
		//ptrnService.close();
		return true;
	}

	@Override
	public List<Object> fetchPattern(String pguid, View view) throws TimeSeriesException {
		IPattern ptrnService = ServiceFactory.getPatternService();
		List<Object> ptrn = ptrnService.fetchPattern(this.clusterName, pguid, view);
		//ptrnService.close();
		return ptrn;
	}
	
	@Override
	public List<String> fetchCapturedPatterns(View view) throws TimeSeriesException {
		IPattern ptrnService = ServiceFactory.getPatternService();
		List<String> capturedPatterns = ptrnService.fetchCapturedPatterns(this.clusterName, view);
		//ptrnService.close();
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
}
