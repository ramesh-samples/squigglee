// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;
//import org.teiid.translator.TranslatorException;







import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.DynamicTypeTranslator;

public class AvroTimeSeriesHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.core.serializers.avro.AvroTimeSeriesHandler");
	private AvroTimeSeriesSchema schema = null;
	//BinaryEncoder encoder = null;
	//BinaryDecoder decoder = null;
	//GenericRecord timeSeriesRecordDeserialized = null;
	//DatumReader<GenericRecord> tsDatumReader = null;
	
	public AvroTimeSeriesHandler() throws TimeSeriesException {
		this.schema = new AvroTimeSeriesSchema();
	}
	
	public AvroTimeSeriesHandler(AvroTimeSeriesSchema schema) {
		this.schema = schema;
	}
	
	public AvroTimeSeriesSchema getSchema() {
		return this.schema;
	}
	
	public GenericRecord deserialize(byte[] message) throws TimeSeriesException {
		if (message == null)
			return null;
		Schema.Type currentType = this.schema.currentDataType;
		BinaryDecoder decoder = null;
		DatumReader<GenericRecord> tsDatumReader = null;
		GenericRecord timeSeriesRecordDeserialized = null;
		try {
			
			//decoder = DecoderFactory.defaultFactory().createBinaryDecoder(message, decoder);
			decoder = DecoderFactory.get().binaryDecoder(message, decoder);
			tsDatumReader = new GenericDatumReader<GenericRecord>(this.schema.getTimeSeriesSchema());
			timeSeriesRecordDeserialized = tsDatumReader.read(timeSeriesRecordDeserialized, decoder);
			return timeSeriesRecordDeserialized;
		} catch (Exception ex) {
			logger.debug("Run time schema does not match current schema datatype = " + currentType);
		}
		//TODO fix this by adding the data type for bulk data to the view definition 
		for (Schema.Type dataType : DynamicTypeTranslator.supportedTypesList) {
			if (dataType.equals(currentType))
				continue;
			this.schema.resetSchema(dataType);
			try {
				//decoder = DecoderFactory.defaultFactory().createBinaryDecoder(message, decoder);
				decoder = DecoderFactory.get().binaryDecoder(message, decoder);
				tsDatumReader = new GenericDatumReader<GenericRecord>(this.schema.getTimeSeriesSchema());
				timeSeriesRecordDeserialized = tsDatumReader.read(timeSeriesRecordDeserialized, decoder);
				return timeSeriesRecordDeserialized;
			} catch (Exception ex) {
				logger.debug("Run time schema does not match current schema datatype = " + currentType);
			}
		}
		
		throw new TimeSeriesException("Run time schema does not match any possible schema types");
	}
	
	public int getOffset(GenericRecord dataRecord) {
		return Integer.parseInt(dataRecord.get("offset").toString());
	}
	
	public Object getDataVal(GenericRecord dataRecord) {
		return dataRecord.get("val");
	}
	
	public Schema.Type getDataValueType(GenericRecord dataRecord) {
		Schema.Type type = dataRecord.getSchema().getField("val").schema().getType();
		return type;
	}
	
	public String getCluster(GenericRecord blockDataRecord) {
		return blockDataRecord.get("cluster").toString();
	}
	
	public String getGuid(GenericRecord blockDataRecord) {
		return blockDataRecord.get("guid").toString();
	}
	
	public int getLn(GenericRecord blockDataRecord) {
		return ((Integer) blockDataRecord.get("ln")).intValue();
	}
	
	public long getStartts(GenericRecord blockDataRecord) {
		return Long.parseLong(blockDataRecord.get("startts").toString());
	}

	public GenericArray<GenericRecord> getBlocks(GenericRecord timeSeriesRecord) {
		return getArrayData(timeSeriesRecord, "block");
	}
	
	public GenericArray<GenericRecord> getData(GenericRecord blockRecord) {
		return getArrayData(blockRecord, "data");
	}
	
	@SuppressWarnings("unchecked")
	private GenericArray<GenericRecord> getArrayData(GenericRecord arrayRecord, String arrayElementName) {
		return (GenericArray<GenericRecord>) arrayRecord.get(arrayElementName);
	}
	
	public byte[] serialize (GenericRecord timeSeriesRecord) throws IOException {
		if (timeSeriesRecord == null)
			return null;
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(this.schema.getTimeSeriesSchema());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
		datumWriter.write(timeSeriesRecord, encoder);
		encoder.flush();
		baos.flush();
		byte[] results = baos.toByteArray(); 
		baos.close();
		encoder = null;
		return results;
	}
	
	public GenericRecord setTimeSeriesRecord(GenericArray<?> blocks) {
		GenericRecord timeSeries = new GenericData.Record(this.schema.getTimeSeriesSchema());
		timeSeries.put("block", blocks);
		return timeSeries;
	}	

	public GenericRecord setBlockRecord(String cluster, int ln, String guid, long startts, GenericArray<?> data) {
		GenericRecord block = new GenericData.Record(this.schema.getBlockSchema());
		block.put("cluster", cluster);
		block.put("ln", ln);
		block.put("guid", guid);
		block.put("startts", startts);
		block.put("data", data);
		return block;
	}

	public GenericRecord setDataRecord(long offset, Object val) {
		GenericRecord data1 = new GenericData.Record(this.schema.getDataSchema());
		data1.put("offset", offset);
		if (val instanceof byte[]) {
			byte[] dataBytes = (byte[]) val;
			data1.put("val", java.nio.ByteBuffer.wrap(dataBytes));
		}
		else
			data1.put("val", val);  
		return data1;
	}
	
}