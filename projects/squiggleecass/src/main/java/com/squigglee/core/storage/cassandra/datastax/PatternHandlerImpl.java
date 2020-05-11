// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
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
	private static Logger logger = Logger.getLogger("com.squigglee.core.storage.cassandra.datastax.PatternIndexHandlerImpl");
	private PreparedStatement psInsertPattern = null;
	private static Map<String,PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
	private static String insertSerializedString = "insert into srlindx (id, pindx, idxbegin, idxend, srlblob) values (?, ?, ?, ?, ?);";
	//private static String insertSerializedString = "update srlindx set idxbegin = ?, idxend = ?, srlblob= ? where id = ? and pindx = ?";
	
	public PatternHandlerImpl(String clusterName, int clusterPort, String clusterSeeds
			, String serializerType, String localDataCenter, String address) throws TimeSeriesException {
		super(clusterName, clusterPort, clusterSeeds, serializerType, localDataCenter, address);
	}
	
	@Override
	public List<Integer> fetchCandidatePatterns(long id, String dataKeyspace,
			String idxTableName, List<String> hashes)
			throws TimeSeriesException {
		List<Integer> candidates = new ArrayList<Integer>();
		for (String hash : hashes) {
			String[] tokens = hash.split("#"); // first k are hashes, last is the projection 
			String select = "select id, val from " + idxTableName + "_" + id  + "_" + tokens[tokens.length-1] + " where id = " + id;
			for (int i=0; i< tokens.length-1; i++)
				select += " and k" + (i+1) + " = " + tokens[i];
			select += ";";
			ResultSet rs = getSession(dataKeyspace).execute(select);
			Iterator<Row> it = rs.iterator();
			while (it.hasNext()) {		
				Row row = it.next();
				if (!candidates.contains(row.getInt("val")))
					candidates.add(row.getInt("val"));
			}
		}
		return candidates;
	}

	@Override
	public Map<Integer, Match> fetchCandidateTimeSeries(long id,	String dataKeyspace, List<Integer> candidates, int size)
			throws TimeSeriesException {
		SortedMap<Integer,Match> output = new TreeMap<Integer,Match>();
		for (int c : candidates) {
			String select = "select offset, val from " + TsrConstants.DATA_CF_NAME + " where id = " + id + " and offset >= " + c + " order by offset asc limit " + size + ";";
			ResultSet rs = getSession(dataKeyspace).execute(select);
			Iterator<Row> it = rs.iterator();
			double[] matchedTs = new double[size];
			int[] offsets = new int[size];
			int cntr = 0;
			while (it.hasNext()) {		
				Row row = it.next();
				
				double data = Double.parseDouble(DynamicTypeTranslator.getDataVal(row, serializer.getDataType()).toString());
				matchedTs[cntr] = data;
				offsets[cntr] = (int) row.getLong("offset");
				cntr++;
			}
			if (cntr == size)
				output.put(c,new Match(id, offsets, matchedTs));
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
			lsh.loadSerializedIndex(md.getId(), md.getKs(), ptrnIndex, this);	 
			List<Integer> candidates = lsh.getStoredIndexedNeighbors(pattern_norm, md.getId(), md.getKs(),  this);
			logger.debug("Found " + candidates.size() + " potential match candidates for node " + md.getLn() + " and parameter " 
					+ md.getGuid() + " and topk = " + topk + " and radius = " + radius);
			System.out.println("Found " + candidates.size() + " potential match candidates for node " + md.getLn() + " and parameter " 
					+ md.getGuid() + " and topk = " + topk + " and radius = " + radius);
			if (candidates.size() > LocalNodeProperties.getMaxMatchCandidates()) {
				logger.debug("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
				System.out.println("Truncating candidates to " + LocalNodeProperties.getMaxMatchCandidates());
				candidates = candidates.subList(0, (LocalNodeProperties.getMaxMatchCandidates() - 1));
			}
			matches = lsh.getStoredExactMatches(pattern_norm, candidates, topk, radius, md.getId(), md.getKs(), this, matches);
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
	public void updatePatternIndex(long id, String dataKeyspace, String idxTableName, int startKey, int endKey,
			Map<String, List<Integer>> lookup, int projHashCount, int projHashNumber) {
		
		BatchStatement bs = new BatchStatement();
		int indexNum = 10000;
		int indexCnt = 0;
		for (String s : lookup.keySet()) {
			for ( int val : lookup.get(s)) {
				if (++indexCnt == 1)
					bs.clear();
				String[] tokens = s.split("#");	//first k hashes and the last is the projection 
				String insert = "insert into " + idxTableName + "_" + id  + "_" + tokens[tokens.length-1] + " (id";
				for (int i=0; i< (tokens.length-1); i++)
					insert += ",k" + (i+1);
				insert += ",val,dummy) values (" + id;
				for (int i=0; i< (tokens.length-1); i++)
					insert += ", " + tokens[i];
				insert += ", " + val + ", null);";
				bs.add(new SimpleStatement(insert));
				if (indexCnt == indexNum) {
					getSession(dataKeyspace).execute(bs);
					indexCnt = 0;
				}
			}
			//System.out.println("Completed Incremental Update for " + s);
		}
		if (bs.getStatements().size() > 0)
			getSession(dataKeyspace).execute(bs);

		if (!getConfigurationStatus(id,idxTableName)) {
			System.out.println("Index no longer configured, skipping update for id = " + id + " index = " + idxTableName);
			logger.debug("Index no longer configured, skipping update for id = " + id + " index = " + idxTableName);
			return;
		}
		
		String insertSerializedString = "insert into srlindx (id, pindx, idxbegin" + projHashNumber + ", idxend" + projHashNumber + ") values (?, ?, ?, ?);";
		if (!psMap.containsKey(dataKeyspace))
			psMap.put(dataKeyspace, getSession(dataKeyspace).prepare(insertSerializedString));
		ResultSet rs = getSession(dataKeyspace).execute(psMap.get(dataKeyspace).bind(id, idxTableName + "_" + id, 0, endKey));

	}

	@Override
	public boolean storePattern(String pguid, int pindx, View view, Object val) throws TimeSeriesException {
		long pid = getPid(pguid, pindx, view);

		String patternInsert = "insert into " + view.name().toLowerCase() + " (pid, pguid, pindx, val) values (?, ?, ?, ?)";
		if (psInsertPattern == null)
			psInsertPattern = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).prepare(patternInsert);
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(psInsertPattern.bind(pid, pguid, pindx, val));
		if (rs != null && rs.wasApplied())
			return true;
		return false;
	}

	@Override
	public List<Object> fetchPattern(String pguid, View view) throws TimeSeriesException {
		String cql = "SELECT pindx, val from " + view.name().toUpperCase() + " where pguid = '" + pguid + "';";
		String cqlType = DynamicTypeTranslator.getViewDataType(view);
		Map<Integer,Object> pattern = new TreeMap<Integer,Object>();
		ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		while (it.hasNext()) {		
			Row row = it.next();
			int indx = row.getInt("pindx");
			Object val = DynamicTypeTranslator.getDataVal(row, DynamicTypeTranslator.getSchemaType(cqlType));
			pattern.put(indx, val);
		}
		return new ArrayList<Object>(pattern.values());
	}
	
	@Override
	public List<String> fetchCapturedPatterns(View view) throws TimeSeriesException {
		String cql = "SELECT pguid from " + view.name().toUpperCase() + " where pindx = 0;";
		List<String> capturedPatterns = new ArrayList<String>();
		try {
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);	
			Iterator<Row> it = rs.iterator();
			while (it.hasNext()) {		
				Row row = it.next();		
				String pguid = row.getString("pguid");
				if (!capturedPatterns.contains(pguid))
					capturedPatterns.add(pguid);
			}
		} catch (Exception ex) {
			logger.error("Could not fetch any captured patterns for view " + view, ex);
		}
		return capturedPatterns;
	}
	
	private long getPid(String pguid, int pindx, View view) {
		//String create = "CREATE TABLE IF NOT EXISTS " + view.name().toUpperCase() + " (pid bigint, pguid text, pindx int, val %s, PRIMARY KEY (pid)) WITH COMPACT STORAGE;";
		//String cqlType = DynamicTypeTranslator.getViewDataType(view);
		//String createTable = String.format(create, cqlType);
		
		//ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(createTable);
		
		//String guidIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." + view.name().toLowerCase() + "(pguid);";
				
		//rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(guidIndexQuery);

		//String pindxIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." + view.name().toLowerCase() + "(pindx);";
		//rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(pindxIndexQuery);
		
		ResultSet rs = null;
		String cql = "SELECT pid, pguid, pindx from " + view.name().toUpperCase() + ";";
		
		rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(cql);
		Iterator<Row> it = rs.iterator();
		long max = 0L;
		while (it.hasNext()) {		
			Row row = it.next();
			long pid = row.getLong("pid");
			String guid = row.getString("pguid");
			int indx = row.getInt("pindx");
			if (guid.equalsIgnoreCase(pguid) && (indx == pindx) )
				return pid;
			if (pid > max)
				max = pid;
		}
		return ++max;
	}
	
	@Override
	public boolean createPatternTable(View view) throws TimeSeriesException {
		try {
			String create = "CREATE TABLE IF NOT EXISTS " + view.name().toUpperCase() + " (pid bigint, pguid text, pindx int, val %s, PRIMARY KEY (pid)) WITH COMPACT STORAGE;";
			String cqlType = DynamicTypeTranslator.getViewDataType(view);
			String createTable = String.format(create, cqlType);
			
			ResultSet rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(createTable);
			
			String guidIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." + view.name().toLowerCase() + "(pguid);";
					
			rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(guidIndexQuery);
	
			String pindxIndexQuery = "CREATE INDEX IF NOT EXISTS ON " + TsrConstants.MASTER_DATA_KEYSPACE_NAME + "." + view.name().toLowerCase() + "(pindx);";
			rs = getSession(TsrConstants.MASTER_DATA_KEYSPACE_NAME).execute(pindxIndexQuery);
		} catch (Exception ex) {
			logger.error("Failed to create table for view " + view, ex);
			throw new TimeSeriesException(ex);
		}
		
		return true;
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
