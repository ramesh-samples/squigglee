package com.squigglee.client.perf;

import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import com.squigglee.api.rest.ITimeSeriesRESTService;
import com.squigglee.api.rest.RESTFactory;
import com.squigglee.client.ClientException;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.TimeSeries;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.synthetic.Zipf;

public class ZipfWriteTest {
	
	static {
		RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
	}
	
	public static void insertRandomZipfInts(int cardinality, String addr, TimeSeriesConfig config, int batchSize, int pause) throws ClientException {
		try {
			if (!config.getDatatype().equalsIgnoreCase("int"))
				throw new ClientException("ZIPF inserts are for int data type only");
			Schema.Type timeSeriesValueType = Schema.Type.INT;
			AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
			handler.getSchema().resetSchema(timeSeriesValueType);
			int counts = 0;
			Zipf generator = new Zipf(cardinality,1.0);
			long sampleCount = TimeSeriesShard.getOffset(config.getFrequency(), config.getStartDate()) - 
					TimeSeriesShard.getOffset(config.getFrequency(), config.getEndDate()) + 1;
			int[] values = generator.sample( (int) sampleCount );
			if (batchSize > values.length)
				batchSize = values.length;
			GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
			ITimeSeriesRESTService service = RESTFactory.getTimeSeriesProxy(addr);
			for (int i=0; i< values.length; i++) {
				dataArray.add(handler.setDataRecord(i, values[i]));
				counts++;
				if ( (counts % batchSize) == 0) {
					service.updateTimeSeriesBulkJSON(getTimeSeries(handler, config.getCluster(), config.getLogicalNode(), 
							config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis(), dataArray));
					System.out.println("Inserted Chunk of " + batchSize + " records at " + DateTime.now() + " ending at offset " 
							+ i + " for node: " + config.getLogicalNode() + " in cluster " + config.getCluster() 
							+ " and parameter " + config.getGuid());
					dataArray.clear();
				}
			}
			if (dataArray.size() > 0) {
				service.updateTimeSeriesBulkJSON(getTimeSeries(handler, config.getCluster(), config.getLogicalNode(), 
						config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis(), dataArray));
				System.out.println("Inserted Chunk of " + batchSize + " records at " + DateTime.now() + " ending at offset " 
						+ (values.length - 1) + " for node: " + config.getLogicalNode() + " in cluster " + config.getCluster() 
						+ " and parameter " + config.getGuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ClientException(e);
		}
	}
	
	public static void insertRandomDoubles(String addr, TimeSeriesConfig config, int batchSize, int pause) throws ClientException {
		try {
			if (!config.getDatatype().equalsIgnoreCase("double"))
				throw new ClientException("Random double inserts are for double data type only");
			Schema.Type timeSeriesValueType = Schema.Type.DOUBLE;
			AvroTimeSeriesHandler handler = new AvroTimeSeriesHandler();
			handler.getSchema().resetSchema(timeSeriesValueType);
			int counts = 0;
			Random rand = new Random();
			
			int sampleCount = (int) (TimeSeriesShard.getOffset(config.getFrequency(), config.getStartDate()) - 
					TimeSeriesShard.getOffset(config.getFrequency(), config.getEndDate()) + 1L);
			double[] values = new double[sampleCount];
			for (int i =0; i< sampleCount; i++)
				values[i] = rand.nextDouble();
			if (batchSize > values.length)
				batchSize = values.length;
			GenericArray<GenericRecord> dataArray = new GenericData.Array<>(1, handler.getSchema().getDataArraySchema());
			ITimeSeriesRESTService service = RESTFactory.getTimeSeriesProxy(addr);
			for (int i=0; i< values.length; i++) {
				dataArray.add(handler.setDataRecord(i, values[i]));
				counts++;
				if ( (counts % batchSize) == 0) {
					service.updateTimeSeriesBulkJSON(getTimeSeries(handler, config.getCluster(), config.getLogicalNode(), 
							config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis(), dataArray));
					System.out.println("Inserted Chunk of " + batchSize + " records at " + DateTime.now() + " ending at offset " 
							+ i + " for node: " + config.getLogicalNode() + " in cluster " + config.getCluster() 
							+ " and parameter " + config.getGuid());
					dataArray.clear();
				}
			}
			if (dataArray.size() > 0) {
				service.updateTimeSeriesBulkJSON(getTimeSeries(handler, config.getCluster(), config.getLogicalNode(), 
						config.getGuid(), config.getStartDate().getMillis(), config.getEndDate().getMillis(), dataArray));
				System.out.println("Inserted Chunk of " + batchSize + " records at " + DateTime.now() + " ending at offset " 
						+ (values.length - 1) + " for node: " + config.getLogicalNode() + " in cluster " + config.getCluster() 
						+ " and parameter " + config.getGuid());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ClientException(e);
		}
	}
	
	private static TimeSeries getTimeSeries(AvroTimeSeriesHandler handler, String cluster, int ln, String id, 
			long startts, long endts, GenericArray<GenericRecord> dataArray) throws IOException {
		TimeSeries ts = new TimeSeries(cluster, ln, id, startts, endts);
		GenericArray<GenericRecord> blockArray = new GenericData.Array<>(1, handler.getSchema().getBlockArraySchema());
		blockArray.add(handler.setBlockRecord(ts.getCluster(), ts.getLn(), ts.getId(), startts, dataArray));
		GenericRecord timeSeriesRecord = handler.setTimeSeriesRecord(blockArray);
		final byte[] serializedBytes = handler.serialize(timeSeriesRecord);
		String bulkData = RESTFactory.encode(serializedBytes);
		ts.setBulkData(bulkData);
		return ts;
	}
	
}
