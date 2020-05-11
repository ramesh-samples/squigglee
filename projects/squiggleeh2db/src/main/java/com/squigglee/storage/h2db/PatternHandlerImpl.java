// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.squigglee.coord.storage.PatternHandlerMixin;
import com.squigglee.core.config.MasterData;
import com.squigglee.core.config.TimeSeriesConfig;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.TimeSeriesException;
import com.squigglee.core.interfaces.View;
import com.squigglee.core.sketch.Match;
import com.squigglee.core.utility.CollectionsUtility;

public class PatternHandlerImpl extends IndexHandlerImpl implements IPatternHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.PatternIndexHandlerImpl");
	protected PatternHandlerMixin phMixin = null;
	
	public PatternHandlerImpl() {
		super();
		this.phMixin = new PatternHandlerMixin(clusterName, ln, this, this.deserializer, this.serializer);
	}
	
	@Override
	public List<Long> fetchCandidatePatterns(MasterData md, String idxTableName, List<Integer> hashes)
			throws TimeSeriesException {
		List<Long> candidates = new ArrayList<Long>();
		Connection conn = getConnection();
		String in = "";
		//for (String queryhash : hashes) {
		//	in += "" + MurmurHash.hash32(queryhash) + ",";
		//}
		if (in.endsWith(","))
			in = in.substring(0,in.length()-1);
		String query = "select vals from " + idxTableName + "_" + md.getId() + " where hashes in (" + in + ");";	
			try {
				ResultSet rs = conn.createStatement().executeQuery(query);
				if (rs != null && rs.next()) {
					String[] vals = rs.getString("vals").split(",");
					for (String val : vals) {
						long v = Long.parseLong(val);
						if (!candidates.contains(v))
						candidates.add(v);
					}
				}
				if (rs != null && !rs.isClosed())
					rs.close();
			} catch (SQLException e) {
				logger.error("Error fetching candidate patterns for master data = " + md + " and query = " + query);
			}
		try {
			if (conn != null && !conn.isClosed())
				conn.close();
			} catch (SQLException e) {
				logger.error("Error closing connection", e);
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
		return phMixin.fetchMatches(list, pguid, view, topk, radius);
		
		/*
		List<Object> pvals = fetchPattern(pguid, getPatternView(view));
		double[] pvalarr = new double[pvals.size()];
		for (int i=0; i< pvalarr.length; i++)
			pvalarr[i] = Double.parseDouble(pvals.get(i).toString());
		double[] normPattern = StatUtils.normalize(pvalarr);
		return fetchMatches(list, normPattern, topk, radius);
		*/
	}
	
	@Override
	public byte[] fetchMatches(List<TimeSeriesConfig> list,	double[] pattern_norm, int topk, double radius)
			throws TimeSeriesException {
		return phMixin.fetchMatches(list, pattern_norm, topk, radius);
		
		/*
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
			for (long key : match.getValues().keySet()) {
				//Object dataVal = DynamicTypeTranslator.parseStringObject( (new Double(match.getValues()[i])).toString(), serializer.getDataType());
				//Object dataVal = DynamicTypeTranslator.convertDoubleToNumeric(match.getValues()[i] , md.getDatatype());
				serializer.setData(key, match.getValues().get(key));
			}
		}
		return serializer.getRawData();
		*/
	}
	
	public void updatePatternIndex(MasterData md, String idxTableName, long startKey, long endKey,
			Map<Integer, List<Long>> lookup, int projHashCount, int projHashNumber) throws TimeSeriesException {

		if (!getConfigurationStatus(md.getId(),idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + md.getId() + " index = " + idxTableName);
			return;
		}
		//Connection conn = getConnection();
		Connection conn = getBatchUpdateConnection();
		PreparedStatement ps1 = null;
		PreparedStatement ps2 = null;
		int indexNum = 5000;
		int indexCnt = 0;
		try {
			//String insert = "insert into " + idxTableName + "_" + md.getId() + " (hashes, vals) values (?,?);";
			String insert = "insert into " + idxTableName + "_" + md.getId() + " SELECT ?,? FROM DUAL WHERE NOT EXISTS (SELECT vals FROM " 
					+ idxTableName + "_" + md.getId() + " where hashes = ?)";
			String update = "update " + idxTableName + "_" + md.getId() + " SET vals = CONCAT_WS(',',vals,?) WHERE hashes = ? and vals is not null";
			ps1 = conn.prepareStatement(insert);
			ps2 = conn.prepareStatement(update);
			for (int s : lookup.keySet()) {
				//int hashl = MurmurHash.hash32(s);
				int hashl = s;
				ps1.setLong(1, hashl);
				ps1.setString(2, CollectionsUtility.getCsv(lookup.get(s)));
				ps1.setInt(3, hashl);
				ps1.addBatch();
				ps2.setString(1, CollectionsUtility.getCsv(lookup.get(s)));
				ps2.setInt(2, hashl);
				ps2.addBatch();
				indexCnt++;
				if (indexCnt == indexNum) {
					try {
						ps2.executeBatch();
						ps2.clearBatch();
						ps1.executeBatch();
						ps1.clearBatch();
						indexCnt = 0;
					} catch (SQLException ex) {
						if (!ex.getMessage().toLowerCase().contains("primary key violation"))
							logger.error("Found error inserting data into pattern index",ex);
					}
				}
			}
			if (indexCnt > 0) {
				try {
					if (ps2 != null) {
						ps2.executeBatch();
						ps2.clearBatch();
					}
					if (ps1 != null) {
						ps1.executeBatch();
						ps1.clearBatch();
					}					
					indexCnt = 0;
				} catch (SQLException ex) {
					if (!ex.getMessage().toLowerCase().contains("primary key violation"))
						logger.error("Found error inserting data into pattern index",ex);
				}
			}
			if (conn != null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			logger.error("Found error inserting data into pattern index",e);
		}
	}
	
	@Override
	public boolean storePattern(String pguid, List<Object> pattern, View view) throws TimeSeriesException {
		return phMixin.storePattern(pguid, pattern, view);
		
		/*
		IPattern ptrnService = ServiceFactory.getPatternService();
		ptrnService.storePattern(this.clusterName, pguid, pattern, view);
		//ptrnService.close();
		return true;
		*/
	}

	@Override
	public List<Object> fetchPattern(String pguid, View view) throws TimeSeriesException {
		return phMixin.fetchPattern(pguid, view);
		
		/*
		IPattern ptrnService = ServiceFactory.getPatternService();
		List<Object> ptrn = ptrnService.fetchPattern(this.clusterName, pguid, view);
		//ptrnService.close();
		return ptrn;
		*/
	}
	
	@Override
	public List<String> fetchCapturedPatterns(View view) throws TimeSeriesException {
		return phMixin.fetchCapturedPatterns(view);
		
		/*
		IPattern ptrnService = ServiceFactory.getPatternService();
		List<String> capturedPatterns = ptrnService.fetchCapturedPatterns(this.clusterName, view);
		//ptrnService.close();
		return capturedPatterns;
		*/
	}
		
	public View getPatternView(View matchView) {
		return phMixin.getPatternView(matchView);
		
		/*
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
		*/
	}

}
