// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.Frequency;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.View;
import com.squigglee.core.interfaces.HandlerFactory;

public class ViewTests extends TestBase {

	@org.junit.Test
	public void verifyAddData() throws Exception {
		
		Double[] data = new Double[]{1.1, 2.2, 3.3, 4.4, 5.5};
		String insert = "INSERT INTO TimeSeriesViewModel." + View.DOUBLES.name() + " (ln,id,ts,val) VALUES (?,?,?,?)";
		PreparedStatement st = viewCon1.prepareStatement(insert);
		for (int i=0; i< data.length; i++) {
			st.setInt(1,config1.getLogicalNode());
			st.setString(2, config1.getGuid());
			st.setTimestamp(3, new java.sql.Timestamp(config1.getStartDate().getMillis() + i));
			st.setDouble(4, data[i]);
			st.addBatch();
		}
		int[] insertResult = st.executeBatch();
		assertEquals(data.length,insertResult[0]);

		String dataQuery = "SELECT ln, id, ts, val FROM TimeSeriesGlobalViewModel.Doubles WHERE ID = '" + config1.getGuid() 
				+ "' and ln = " + config1.getLogicalNode()
				+ " and ts >= ? and ts <= ?"; 
		PreparedStatement ps = globalViewCon.prepareStatement(dataQuery);
		ps.setTimestamp(1, new java.sql.Timestamp(config1.getStartDate().getMillis()));
		ps.setTimestamp(2, new java.sql.Timestamp(config1.getStartDate().getMillis() + 4));
		//ResultSet rs = viewCon.createStatement().executeQuery(dataQuery);
		ResultSet rs = ps.executeQuery();
		Double[] tsValues = new Double[5];
		int cnt = 0;
		while (rs.next()) {
			tsValues[cnt++] = rs.getDouble("val");
		}
		assertArrayEquals(data,tsValues);
	}
	
	@org.junit.Test
	public void verifyCMSketch() throws Exception {
		//List<Integer> data = getSampleZipfData();
		MasterData md = HandlerFactory.getMasterDataHandler().getMasterData(config2.getCluster(), config2.getLogicalNode(), config2.getGuid(), config2.getStartDate().getMillis());
		Map<Long,Object> data = HandlerFactory.getDataHandler().fetchTimeSeries(md, 
				(int) config2.getStartDate().getMillis(), (int) config2.getEndDate().getMillis());
		Map<Integer,Integer> freqMap = new HashMap<Integer,Integer>();
		for (Object o : data.values()) {
			int val = Integer.parseInt(o.toString());
			if (freqMap.containsKey(val))
				freqMap.put(val, freqMap.get(val) + 1);
			else
				freqMap.put(val, 1);
		}
		
		int topk = 10;
		List<Integer[]> topkList = new ArrayList<Integer[]>();
		for (int value : freqMap.keySet()) {
			int freq = freqMap.get(value);
			if (topkList.size() < topk) {
				topkList.add(new Integer[]{value,freq});
			}
			else {
				for (int k1=0; k1 < topkList.size(); k1++) {
					if (freq > topkList.get(k1)[1]) {
						topkList.add(k1, new Integer[]{value,freq});
						break;
					}
				}
			}
			if (topkList.size() > topk)
				topkList.remove(topkList.size()-1);
		}
		
		double error = 0.0;
		for (int i=0; i< topkList.size(); i++) {
			System.out.println(topkList.get(i)[0]);
			String pointQuery = "SELECT ln, id, freq, val FROM TimeSeriesViewModel." + View.SKETCHEDINTEGERS + 
					" where ln = " + config2.getLogicalNode() + " and id = '" + config2.getGuid() 
					+ "' and val = " + topkList.get(i)[0] + " and st = '" + "skchCM'";
			ResultSet skrs = viewCon1.createStatement().executeQuery(pointQuery);
			while (skrs.next()) {
				double pointfreq = skrs.getDouble("freq");
				double dataFreq = freqMap.get(topkList.get(i)[0])*1.0/data.size();
				System.out.println(pointfreq + " -- " + freqMap.get(topkList.get(i)[0])*1.0/data.size());
				assertTrue( (pointfreq - freqMap.get(topkList.get(i)[0])*1.0/data.size()) <= 0.0000001);
				error += Math.abs(dataFreq - pointfreq)/dataFreq;
			}
		}
		assertTrue(error <= 0.000001);
	}

	@org.junit.Test
	public void verifyBulkData() throws Exception {
		
		double[] data = new double[]{1.1, 2.2, 3.3, 4.4, 5.5};
		Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int j = 0; j < data.length; j++)
			dataArray.add(handler.setDataRecord(j, data[j]));
		
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(config1.getCluster(), config1.getLogicalNode(), config1.getGuid(), config1.getStartDate().getMillis(), dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		
		String insertBulkQuery = "INSERT INTO TimeSeriesViewModel." + View.BULKDATA.name() + 
				" (ln,id,datatype,bdata) VALUES (?,?,?,?) ";
		PreparedStatement is = viewCon1.prepareStatement(insertBulkQuery);
		is.setInt(1,config1.getLogicalNode());
		is.setString(2, config1.getGuid());
		is.setString(3, config1.getDatatype());
		is.setBytes(4, serializedBytes);
		int insertCount = is.executeUpdate();
		assertEquals(data.length, insertCount);
		
		String bulkSelectQuery = "SELECT ln, id, startdt, enddt, datatype, bdata from TimeSeriesViewModel." + View.BULKDATA.name() +
				" WHERE ln = " + config1.getLogicalNode() + " and id = '" + config1.getGuid() + "'" + 
				" and startdt = ? and enddt = ?";
		PreparedStatement ps = viewCon1.prepareStatement(bulkSelectQuery);
		ps.setTimestamp(1, new java.sql.Timestamp(config1.getStartDate().getMillis()));
		ps.setTimestamp(2, new java.sql.Timestamp(config1.getStartDate().getMillis() + 4));
		ResultSet bqrs = ps.executeQuery();
		while (bqrs.next()) {
			assertArrayEquals(serializedBytes, bqrs.getBytes("bdata"));
			assertEquals(config1.getStartDate().getMillis(), bqrs.getTimestamp("startdt").getTime());
			assertEquals(config1.getDatatype(), bqrs.getString("datatype"));
			assertEquals(config1.getStartDate().getMillis()+4, bqrs.getTimestamp("enddt").getTime());
		}		
	}

	@org.junit.Test
	public void verifyMasterData() throws Exception {
		
		String mdQuery = "SELECT id, logicalnode, replication, strategy, frequency, datatype, indexes, " + 
				"storagestart, storageend FROM TimeSeriesViewModel." + View.MASTERDATA.name() + " WHERE ID = '" 
				+ config1.getGuid() + "' and logicalnode = " + config1.getLogicalNode() + ";";
		ResultSet rs = viewCon1.createStatement().executeQuery(mdQuery);
		
		int cnt = 0;
		while (rs.next()) {
			cnt++;
			assertEquals(1,cnt);
			assertEquals(config1.getGuid(),rs.getString("id"));
			assertEquals(config1.getDatatype(),rs.getString("datatype"));
			assertEquals(config1.getLogicalNode(),rs.getInt("logicalnode"));
			assertEquals(config1.getFrequency(),Frequency.valueOf(rs.getString("frequency")));
			assertEquals(config1.getDatatype(),rs.getString("datatype"));
			assertEquals(config1.getIndexes(),rs.getString("indexes"));		
		}
		
		java.sql.Timestamp start = new java.sql.Timestamp(DateTimeHelper.parseDateString("2014-11-22T00:00:00.000Z").getMillis());
		java.sql.Timestamp end = new java.sql.Timestamp(DateTimeHelper.parseDateString("2014-11-22T00:59:59.999Z").getMillis());
		mdQuery = "insert into TimeSeriesViewModel.MASTERDATA (ID,LOGICALNODE,REPLICATION,STRATEGY,FREQUENCY,DATATYPE,INDEXES,STORAGESTART,STORAGEEND) " + 
				" VALUES ('Demo_Days', 0, 'replication_factor:1', 'SimpleStrategy', 'MILLIS', 'double', null, ?, ?);";
		PreparedStatement ps = viewCon1.prepareStatement(mdQuery);
		ps.setTimestamp(1, start);
		ps.setTimestamp(2, end);
		int insertResults = ps.executeUpdate();
		assertEquals(1,insertResults);
		
		mdQuery = "delete from TimeSeriesViewModel.MASTERDATA WHERE LOGICALNODE = 0 and ID = 'Demo_Days' and STORAGESTART = ? and STORAGEEND = ?;";
		ps = viewCon1.prepareStatement(mdQuery);
		ps.setTimestamp(1, start);
		ps.setTimestamp(2, end);
		int deleteResults = ps.executeUpdate();
		assertEquals(1,deleteResults);
	
	}

	@org.junit.Test
	public void verifyPatternIndexing() throws Exception {
	
		List<Double> data = getSampleEKGData(); // already stored 
		//String pguid = java.util.UUID.randomUUID().toString();
		String pguid = "SamplePattern1";
		int startIndex = 103;
		//the 11 through 26 values in the test data set 
		Double[] pattern = new Double[16];
		double[] ptrn = new double[16];
		for (int i=0; i<pattern.length; i++) {
			pattern[i] = data.get(startIndex + i);
			ptrn[i] = data.get(startIndex + i);
		}
	
		String insertPatternQuery = "INSERT INTO TimeSeriesViewModel.PatternDoubles (pguid,pindx,val) values (?,?,?);"; 
		PreparedStatement ps = viewCon1.prepareStatement(insertPatternQuery);
		for (int i=0; i<pattern.length; i++) {
			ps.setString(1, pguid);
			ps.setInt(2, i);
			ps.setDouble(3, pattern[i]);
			int insertResult = ps.executeUpdate();
			assertEquals(1, insertResult);
		}
		Double[] fetchedPattern = new Double[pattern.length];		
		String patternSelectQuery = "SELECT val from TimeSeriesViewModel.PatternDoubles where pguid ='" + pguid + "';";
		ResultSet prs = viewCon1.createStatement().executeQuery(patternSelectQuery);
		int count = 0;
		while (prs.next()) {
			fetchedPattern[count++] = prs.getDouble("val");
		}
		assertArrayEquals(pattern,fetchedPattern);
		
		String matchQuery = "SELECT ln, id, ts, val, pid, rank, radius from TimeSeriesViewModel.MatchedDoubles where " + 
		"id = '" + config1.getGuid() + "' and ln = " + config1.getLogicalNode() + " and rank <= 3 and radius = 0.05 and pid = '" + pguid + "';";
		ResultSet mrs = viewCon1.createStatement().executeQuery(matchQuery);
		count = 0;
		while (mrs.next()) {
			assertArrayEquals(new Double[]{mrs.getDouble("val")},new Double[]{pattern[count]});
			assertEquals(mrs.getInt("rank"),1);
			assertEquals(mrs.getString("pid"),pguid);
			assertEquals(mrs.getString("id"),config1.getGuid());
			assertArrayEquals(new Double[]{mrs.getDouble("radius")},new Double[]{0.05});
			++count;
		}
		assertEquals(count, 16);
	}

	@org.junit.Test
	public void verifySampledData() throws Exception {
		Double[] data = new Double[]{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10};
		
		String insert = "INSERT INTO TimeSeriesViewModel." + View.DOUBLES.name() + " (ln,id,ts,val) VALUES (?,?,?,?)";
		PreparedStatement st = viewCon1.prepareStatement(insert);
		for (int i=0; i< data.length; i++) {
			st.setInt(1,config1.getLogicalNode());
			st.setString(2, config1.getGuid());
			st.setTimestamp(3, new java.sql.Timestamp(config1.getStartDate().getMillis() + i));
			st.setDouble(4, data[i]);
			st.addBatch();
		}
		int[] insertResult = st.executeBatch();
		assertEquals(data.length,insertResult[0]);
	
		
		String dataQuery = "SELECT ln, id, ts, val FROM TimeSeriesViewModel.SampledDoubles WHERE ID = '" + config1.getGuid() 
				+ "' and ln = " + config1.getLogicalNode()
				+ " and ts >= ? and ts <= ? and sf = 0.50;"; 
		PreparedStatement ps = viewCon1.prepareStatement(dataQuery);
		ps.setTimestamp(1, new java.sql.Timestamp(config1.getStartDate().getMillis()));
		ps.setTimestamp(2, new java.sql.Timestamp(config1.getStartDate().getMillis() + 9));
		//ResultSet rs = viewCon.createStatement().executeQuery(dataQuery);
		ResultSet rs = ps.executeQuery();
		int cnt = 0;
		List<Double> dataList = Arrays.asList(data);
		while (rs.next()) {
			//System.out.println(cnt + " -- " + rs.getDouble("val"));
			++cnt;
			assertTrue(dataList.contains(rs.getDouble("val")));
		}
		assertEquals(5,cnt);
		//assertArrayEquals(data,tsValues);
	}

	@org.junit.Test
	public void verifyMultipleFrequencies() throws Exception {
		List<Integer> data = new ArrayList<Integer>();
		data.add(0);data.add(1);data.add(2);data.add(3);data.add(4);
		int microOffset = 899;
		int nanoOffset = 734666;
		int dataOffset;
		for (Frequency freq : freqConfigs.keySet()) {
			dataOffset = 0;
			
			//if (!freq.equals(Frequency.NANOS))
			//	continue;
			
			if (freq.equals(Frequency.MICROS))
				dataOffset = microOffset;
			else if (freq.equals(Frequency.NANOS))
				dataOffset = nanoOffset;
			
			String insertBulkQuery = "INSERT INTO TimeSeriesViewModel." + View.INTEGERS.name() + 
					" (ln,id,ts,off,val) VALUES (?,?,?,?,?) ";
			PreparedStatement is = viewCon1.prepareStatement(insertBulkQuery);
			DateTime current = freqConfigs.get(freq).getStartDate();
			for (int j = 0; j < data.size(); j++)  {
				is.setInt(1,freqConfigs.get(freq).getLogicalNode());
				is.setString(2, freqConfigs.get(freq).getGuid());
				is.setTimestamp(3, new java.sql.Timestamp(current.getMillis()) );
				is.setInt(4, dataOffset + j);
				is.setInt(5, data.get(j));
				is.addBatch();
				current = TimeSeriesShard.advance(freq, current,1);
			}
			int insertCount = is.executeBatch()[0];
			assertEquals(data.size(), insertCount);
	
			String bulkSelectQuery = "SELECT ln, id, ts, off, val from TimeSeriesViewModel." + View.INTEGERS.name() +
					" WHERE ln = " + freqConfigs.get(freq).getLogicalNode() + " and id = '" + freqConfigs.get(freq).getGuid() + "'" + 
					" and ts >= ? and off >= ? and ts <= ? and off <= ?";
			PreparedStatement ps = viewCon1.prepareStatement(bulkSelectQuery);
			ps.setTimestamp(1, new java.sql.Timestamp(freqConfigs.get(freq).getStartDate().getMillis()));
			ps.setInt(2, 0 + dataOffset);
			ps.setTimestamp(3, new java.sql.Timestamp(freqConfigs.get(freq).getEndDate().getMillis()));
			ps.setInt(4, 4 + dataOffset);
			ResultSet bqrs = ps.executeQuery();
			int fetchedCount = 0;
			current = freqConfigs.get(freq).getStartDate();
			//System.out.println("Verifying for " + freqConfigs.get(freq).getGuid());
			while (bqrs.next()) {
				//System.out.println(bqrs.getInt("ln") + "--" + bqrs.getString("id") + "--" + bqrs.getTimestamp("ts") + "--" + bqrs.getInt("off") + "--" + bqrs.getInt("val"));
				if (TimeSeriesShard.ignoreOffsets(freq))
					assertEquals(current.getMillis(), bqrs.getTimestamp("ts").getTime());
				else
					assertEquals(fetchedCount + dataOffset, bqrs.getInt("off"));
				assertEquals(data.get(fetchedCount).intValue(), bqrs.getInt("val"));
				assertEquals(freqConfigs.get(freq).getLogicalNode(), bqrs.getInt("ln"));
				assertEquals(freqConfigs.get(freq).getGuid(), bqrs.getString("id"));
				fetchedCount++;
				current = TimeSeriesShard.advance(freq, current,1);
			}
			assertEquals(fetchedCount,data.size());
			//System.out.println("Success for " + freqConfigs.get(freq).getGuid());
		}
	}

	@org.junit.Test
	public void verifyMultipleFrequenciesBulk() throws Exception {
		int[] data = new int[]{0, 1, 2, 3, 4};
		Schema.Type timeSeriesValueType = Schema.Type.INT;
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		int microOffset = 899;
		int nanoOffset = 734666;
		int dataOffset;
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		for (Frequency freq : freqConfigs.keySet()) {
			dataOffset = 0;
			if (freq.equals(Frequency.MICROS))
				dataOffset = microOffset;
			else if (freq.equals(Frequency.NANOS))
				dataOffset = nanoOffset;
			
			blockArray.clear();
			dataArray.clear();
			for (int j = 0; j < data.length; j++)
					dataArray.add(handler.setDataRecord(j + dataOffset, data[j]));
			blockArray.add(handler.setBlockRecord(freqConfigs.get(freq).getCluster(), freqConfigs.get(freq).getLogicalNode(), freqConfigs.get(freq).getGuid(), freqConfigs.get(freq).getStartDate().getMillis(), dataArray));
			GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
			byte[] serializedBytes = handler.serialize(timeSeriesRecord);
			
			String insertBulkQuery = "INSERT INTO TimeSeriesViewModel." + View.BULKDATA.name() + 
					" (ln,id,datatype,bdata) VALUES (?,?,?,?) ";
			PreparedStatement is = viewCon1.prepareStatement(insertBulkQuery);
			is.setInt(1,freqConfigs.get(freq).getLogicalNode());
			is.setString(2, freqConfigs.get(freq).getGuid());
			is.setString(3, freqConfigs.get(freq).getDatatype());
			is.setBytes(4, serializedBytes);
			int insertCount = is.executeUpdate();
			assertEquals(data.length, insertCount);
			
			String bulkSelectQuery = "SELECT ln, id, startdt, startoffset, enddt, endoffset, datatype, bdata from TimeSeriesViewModel." + View.BULKDATA.name() +
					" WHERE ln = " + freqConfigs.get(freq).getLogicalNode() + " and id = '" + freqConfigs.get(freq).getGuid() + "'" + 
					" and startdt = ? and startoffset = ? and enddt = ? and endoffset = ?";
			PreparedStatement ps = viewCon1.prepareStatement(bulkSelectQuery);
			ps.setTimestamp(1, new java.sql.Timestamp(freqConfigs.get(freq).getStartDate().getMillis()));
			ps.setInt(2, 0 + dataOffset);
			ps.setTimestamp(3, new java.sql.Timestamp(freqConfigs.get(freq).getEndDate().getMillis()));
			ps.setInt(4, 4 + dataOffset);
			
			ResultSet bqrs = ps.executeQuery();
			while (bqrs.next()) {
				//System.out.println("Verifying bulk for " + freqConfigs.get(freq).getGuid());
				assertArrayEquals(serializedBytes, bqrs.getBytes("bdata"));
				assertEquals(freqConfigs.get(freq).getStartDate().getMillis(), bqrs.getTimestamp("startdt").getTime());
				assertEquals(dataOffset, bqrs.getInt("startoffset"));
				assertEquals(freqConfigs.get(freq).getDatatype(), bqrs.getString("datatype"));
				assertEquals(freqConfigs.get(freq).getEndDate().getMillis(), bqrs.getTimestamp("enddt").getTime());
				assertEquals(dataOffset+4, bqrs.getInt("endoffset"));
				//System.out.println("Success for " + freqConfigs.get(freq).getGuid());
			}	
		}
	}
}