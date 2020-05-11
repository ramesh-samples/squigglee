package com.squigglee.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;

import com.squigglee.client.cl.CommandLineHelper;
import com.squigglee.core.config.DateTimeHelper;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.serializers.avro.AvroTimeSeriesSerializer;
import com.squigglee.core.interfaces.HandlerFactory;
import com.squigglee.core.synthetic.Zipf;
import com.squigglee.core.utility.TaskUtility;

public class DirectBulkDataInsert {

	String vdbGlobalName = "TIMESERIESGLOBAL";
	String vdbLocalName = "TIMESERIES";
	//static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	static String localBulkInsertStatement = "INSERT INTO TimeSeriesViewModel.BULKDATA" + 
							" (ln,id,datatype,bdata) VALUES (?,?,?,?)";
	static {
		try {
			Class.forName("org.teiid.jdbc.TeiidDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) throws ClientException {
		Options options = CommandLineHelper.getAllOptions();
		CommandLineParser parser = new GnuParser();
		DirectBulkDataInsert bdi = new DirectBulkDataInsert();
		int count = 0;
		try {
			CommandLine line = parser.parse( options, args );
	        if (line.getOptionValue("method").equalsIgnoreCase("ZIPF"))
	        	count = bdi.bulkZipfRandomInsert(line);
	        else if (line.getOptionValue("method").equalsIgnoreCase("CPAIR"))
	        	count = bdi.bulkCurrencyPairFileInsert(line);
	        else
	        	count = bdi.bulkFileInsert(line);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		System.out.println("Final Bulk Data TimeSeriesClient Count = " + count);
		System.exit(0);
	}
	
	public int bulkZipfRandomInsert(CommandLine cl) throws ClientException, SQLException, ParseException, TimeSeriesException, IOException {
		IDataHandler dataHandler = null;
		
		int pause = Integer.parseInt(cl.getOptionValue("pause"));
		
		DateTime start = DateTimeHelper.parseDateString(cl.getOptionValue("start"));
		DateTime end = DateTimeHelper.parseDateString(cl.getOptionValue("end"));
		long startOffset = 0;
		long endOffset = end.getMillis() - start.getMillis();	
		Schema.Type timeSeriesValueType = Schema.Type.INT;		//currently only int for ZIPF random data
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(timeSeriesValueType);
		int counts = 0;
		Zipf generator = new Zipf(1000,1.0);	
		int[] values = generator.sample( (int) (endOffset - startOffset + 1) );
		int batchSize = Integer.parseInt(cl.getOptionValue("batchsize"));
		if (batchSize > values.length)
			batchSize = values.length;
		int startoffset = Integer.parseInt(cl.getOptionValue("offset"));
		GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		for (int i=0; i< values.length; i++) {
			if (i < startoffset)
				continue;
			dataArray.add(handler.setDataRecord(i, values[i]));
			counts++;
			boolean inserted = false;
			if ( (counts % batchSize) == 0) {
				GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
				blockArray.add(handler.setBlockRecord(cl.getOptionValue("cluster"), Integer.parseInt(cl.getOptionValue("ln")), 
						cl.getOptionValue("id"), start.getMillis(), dataArray));
				GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
				final byte[] serializedBytes = handler.serialize(timeSeriesRecord);
				int retry = 0;
				int maxRetry = 15;
				while (retry <= maxRetry) {
					try {
						dataHandler = HandlerFactory.getDataHandler();
						dataHandler.reset("int");
						dataHandler.insertBulkData(serializedBytes);
						inserted = true;
						retry = 0;
						break;
					} catch (Exception ex) {
						retry++;
						ex.printStackTrace();
						if (retry > maxRetry) {
							System.out.println("Failures have exceeeded the max retry count of " + maxRetry + ", ... exiting");
							System.exit(1);
						} else {
							System.out.println(" Exception while inserting, will sleep and try again for retry count = " + retry);
							//release all resources & re-obtain
							try {
								TaskUtility.sleep(120);		//2 minutes
							} catch (Exception inner) {
								inner.printStackTrace();
							}
						}
					} finally {
						dataHandler.shutdown();
					}
				}
				if (inserted)
					System.out.println("Inserted Chunk of " + batchSize + " records at " + DateTime.now() + " ending at offset: " + i + " for node: " 
						+ cl.getOptionValue("ln") + " and parameter: " + cl.getOptionValue("id"));
				dataArray.clear();
				TaskUtility.sleep(pause);
			}
		}
		return counts;
	}
	
	public int bulkCurrencyPairFileInsert(CommandLine cl) throws ClientException, SQLException, ParseException, TimeSeriesException, IOException {
		IDataHandler dataHandler = HandlerFactory.getDataHandler();
		
		Type schemaType = DynamicTypeTranslator.getSchemaType(cl.getOptionValue("datatype"));
		//each array --> date in millis, the nano offset, the bid, and the ask
		List<String[]> chunkOfData = new ArrayList<String[]>();
		BufferedReader br = null;
		try {
			String line = null; 
			br = new BufferedReader(new FileReader(cl.getOptionValue("file")));
			while ((line = br.readLine()) != null) {
				if (line.toLowerCase().contains("ratedatetime,ratebid,rateask"))			// skip header line
					continue;
				String[] row = line.split(",");
				if (row == null || row.length != 6)
					continue;
				String dateString = null;
				String offsetString = "000000";
				if (row[3].length() > 19) {
					dateString = row[3].substring(0,row[3].length()-6);
					dateString = dateString.replace(" ", "T") +  "Z";
					offsetString = row[3].substring(row[3].length()-6);
				}
				else
					dateString = row[3].replace(" ", "T") + ".000Z";
				chunkOfData.add(new String[]{dateString, offsetString, row[4],row[5]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		
		if (chunkOfData.size() == 0)
			return 0;
		int pause = Integer.parseInt(cl.getOptionValue("pause"));
		
		AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
		handler.getSchema().resetSchema(schemaType);
		int counts = 0;

		GenericArray<GenericRecord> bidDataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		GenericArray<GenericRecord> askDataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		GenericArray<GenericRecord> spreadDataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
		DateTime startDate = DateTimeHelper.parseDateString(chunkOfData.get(0)[0]);
		int counter = 0;
		boolean dayChanged = false;
		for (int i=0; i< chunkOfData.size(); i++) {
			DateTime parsedDate = DateTimeHelper.parseDateString(chunkOfData.get(i)[0]);
			long offset = parsedDate.getMillis() - startDate.getMillis();
			if (parsedDate.getDayOfYear() == startDate.getDayOfYear()) {
				//skip nanosecond offset for now & process as millis 
				bidDataArray.add(handler.setDataRecord(offset, new Double(Double.parseDouble(chunkOfData.get(i)[2]))));
				askDataArray.add(handler.setDataRecord(offset, new Double(Double.parseDouble(chunkOfData.get(i)[3]))));
				//converted to pips w/2 fractional digits --> one pip = 0.0001
				spreadDataArray.add(handler.setDataRecord(offset, Math.round( ((Double.parseDouble(chunkOfData.get(i)[3]) - Double.parseDouble(chunkOfData.get(i)[2]))*10000) * 100.0)/100.0));		
			} else {
				dayChanged = true;
			}
			counter++;
			if ( (counter % Integer.parseInt(cl.getOptionValue("batchsize")) == 0) || dayChanged || i == (chunkOfData.size()-1)) {
				GenericArray<GenericRecord> bidBlockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
				bidBlockArray.add(handler.setBlockRecord(cl.getOptionValue("cluster"), Integer.parseInt(cl.getOptionValue("ln")), cl.getOptionValue("id") + "_bid", startDate.getMillis(), bidDataArray));
				GenericRecord bidTimeSeriesRecord = handler.setTimeSeriesRecord(bidBlockArray);
				final byte[] serializedBidBytes = handler.serialize(bidTimeSeriesRecord);
				
				//GenericArray<GenericRecord> askBlockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
				//askBlockArray.add(handler.setBlockRecord(cl.getOptionValue("cluster"), Integer.parseInt(cl.getOptionValue("ln")), cl.getOptionValue("id") + "_ask", startDate.getMillis(), askDataArray));
				//GenericRecord askTimeSeriesRecord = handler.setTimeSeriesRecord(askBlockArray);
				//final byte[] serializedAskBytes = handler.serialize(askTimeSeriesRecord);
				
				//GenericArray<GenericRecord> spreadBlockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
				//spreadBlockArray.add(handler.setBlockRecord(cl.getOptionValue("cluster"), Integer.parseInt(cl.getOptionValue("ln")), cl.getOptionValue("id") + "_spread", startDate.getMillis(), spreadDataArray));
				//GenericRecord spreadTimeSeriesRecord = handler.setTimeSeriesRecord(spreadBlockArray);
				//final byte[] serializedSpreadBytes = handler.serialize(spreadTimeSeriesRecord);
				
				int retry = 0;
				int maxRetry = 15;
				int count = 0;
				while (retry <= maxRetry) {
					try {
						dataHandler.initialize();
						if (dataHandler.insertBulkData(serializedBidBytes))
							count += Integer.parseInt(cl.getOptionValue("batchsize"));
						dataHandler.shutdown();
						retry = 0;
						break;
					} catch (Exception ex) {
						retry++;
						ex.printStackTrace();
						if (retry > maxRetry) {
							System.out.println("Failures have exceeeded the max retry count of " + maxRetry + ", ... exiting");
							System.exit(1);
						} else {
							System.out.println(" Exception while inserting, will sleep and try again for retry count = " + retry);
							//release all resources & re-obtain
							try {
								dataHandler.shutdown();
								TaskUtility.sleep(120);		//2 minutes
								dataHandler.initialize();
							} catch (Exception inner) {
								inner.printStackTrace();
							}
						}
					}
				}
				System.out.println("Inserted Chunk of " + count + " records at " + DateTime.now() + " ending at date: " + chunkOfData.get(dayChanged?(i-1):i)[0] + " for node: " 
				+ cl.getOptionValue("ln") + " and parameter: " + cl.getOptionValue("id") + " for bid, ask, and spreads");
				counts += count;
				bidDataArray.clear();askDataArray.clear();spreadDataArray.clear();
				TaskUtility.sleep(pause);
			}
			if (dayChanged) {
				startDate = parsedDate; 
				bidDataArray.add(handler.setDataRecord(offset, new Double(Double.parseDouble(chunkOfData.get(i)[2]))));
				askDataArray.add(handler.setDataRecord(offset, new Double(Double.parseDouble(chunkOfData.get(i)[3]))));
				spreadDataArray.add(handler.setDataRecord(offset, new Double( Double.parseDouble(chunkOfData.get(i)[3]) - Double.parseDouble(chunkOfData.get(i)[2]) )));
				dayChanged = false;
			}
		}
		dataHandler.shutdown();
		return counts;
	}
	
	public int bulkFileInsert(CommandLine cl) throws ClientException, SQLException, ParseException, TimeSeriesException {
		String url = "jdbc:teiid:" + vdbLocalName + "@mm://" + cl.getOptionValue("server") + ":" + cl.getOptionValue("port");
		Connection c1 =  DriverManager.getConnection(url, cl.getOptionValue("user"), cl.getOptionValue("userpw"));	
		
		//SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		//DateTime start = new DateTime(sdf.parse(cl.getOptionValue("start")).getTime(),DateTimeZone.UTC);
		//DateTime end = new DateTime(sdf.parse(cl.getOptionValue("end")).getTime(),DateTimeZone.UTC);
		DateTime start = new DateTime(DateTimeHelper.parseDateString(cl.getOptionValue("start")));
		DateTime end = new DateTime(DateTimeHelper.parseDateString(cl.getOptionValue("end")));
		
		
		//long startOffset = 0;
		//long endOffset = end.getMillis() - start.getMillis();
		AvroTimeSeriesSerializer serializer = new AvroTimeSeriesSerializer();
		//serializer.reset();
		Type schemaType = DynamicTypeTranslator.getSchemaType(cl.getOptionValue("datatype"));
		//serializer.resetSchema(Schema.Type.DOUBLE);
		int counts = 0;
		//int chunkSize = cl.getBatchSize();
		List<List<Object>> chunkOfData = new ArrayList<List<Object>>();
		chunkOfData.add(new ArrayList<Object>());
		chunkOfData.add(new ArrayList<Object>());
		BufferedReader br = null;
		try {
			String line = null; 
			br = new BufferedReader(new FileReader(cl.getOptionValue("file")));
			int counter = 0;
			while ((line = br.readLine()) != null) {
				String[] row = line.split(",");
				if (row == null || row.length != 3)
					continue;
				chunkOfData.get(0).add(Long.parseLong(row[1]));
				chunkOfData.get(1).add(DynamicTypeTranslator.parseStringObject(row[2],schemaType));
				counter++;
				if (counter % Integer.parseInt(cl.getOptionValue("batchsize")) == 0) {
					//c1 = DriverManager.getConnection(url, cl.getTeiidUser(), cl.getTeiidCredential());
					PreparedStatement ps = c1.prepareStatement(localBulkInsertStatement);
					int count = insertChunk(chunkOfData,serializer, start, end, schemaType, cl.getOptionValue("cluster"), 
							Integer.parseInt(cl.getOptionValue("logicalnumber")), cl.getOptionValue("id"), ps);
					System.out.println("Inserted Chunk of " + count + " records");
					counts += count;
					chunkOfData.get(0).clear();
					chunkOfData.get(1).clear();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		c1.close();
		return counts;
	}	
	
	public int bulkInsert(String vdbName, String server, int port, String user, String credential,
			DateTime start, DateTime end, String dataType, String cluster, int ln, String guid, List<List<Object>> data) 
			throws SQLException, ClassNotFoundException, ParseException, TimeSeriesException {

		String url = "jdbc:teiid:" + vdbName + "@mm://" + server + ":" + port;
		Connection c1 =  DriverManager.getConnection(url, user, credential);	
		AvroTimeSeriesSerializer serializer = new AvroTimeSeriesSerializer();
		Type schemaType = DynamicTypeTranslator.getSchemaType(dataType);
		PreparedStatement ps = c1.prepareStatement(localBulkInsertStatement);
		int count = insertChunk(data,serializer, start, end, schemaType, cluster, ln, guid, ps);	
		c1.close();
		return count;
	}
	
	private int insertChunk(List<List<Object>> chunkOfData, AvroTimeSeriesSerializer serializer, DateTime start, 
			DateTime end, Type schemaType, String cluster, int ln, String guid, PreparedStatement ps) throws SQLException, TimeSeriesException {
		
		serializer.reset();
		serializer.resetSchema(schemaType);
		serializer.setBlockCount(1);
		serializer.startNewBlock(cluster, ln, guid, start.getMillis(), chunkOfData.get(0).size());
		ps.setInt(1, ln);
		ps.setString(2, guid);
		ps.setTimestamp(3, new java.sql.Timestamp(start.getMillis()));
		ps.setTimestamp(4, new java.sql.Timestamp(end.getMillis()));
		prepareData(chunkOfData, serializer, ps);
		return ps.executeUpdate();
	}
	
	private void prepareData(List<List<Object>> chunkOfData, AvroTimeSeriesSerializer serializer, PreparedStatement ps) 
			throws SQLException, TimeSeriesException {

		List<Object> offsets = (List<Object>) chunkOfData.get(0);
		List<Object> data = (List<Object>) chunkOfData.get(1);
			for (int i = 0; i < offsets.size(); i++)
				serializer.setData( ((Long) offsets.get(i)).longValue(), data.get(i) );
			final byte[] bulkData = serializer.getRawData();
			ps.setObject(5, new org.teiid.core.types.BlobImpl(new org.teiid.core.types.InputStreamFactory() {
	            @Override
	            public InputStream getInputStream() throws IOException {
	                return new ByteArrayInputStream(bulkData);
	            }
	        })); 
	}
}
