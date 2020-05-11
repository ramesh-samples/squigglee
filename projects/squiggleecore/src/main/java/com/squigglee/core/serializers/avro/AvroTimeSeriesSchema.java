// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
//import org.teiid.translator.TranslatorException;







import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.serializers.DynamicTypeTranslator;

public class AvroTimeSeriesSchema {

	Schema timeSeriesSchema;
	Schema blockArraySchema;
	Schema blockSchema;
	Schema dataArraySchema;
	Schema dataSchema;
	Schema.Type currentDataType;
	
	public AvroTimeSeriesSchema() throws TimeSeriesException {
		resetSchema(Schema.Type.DOUBLE);
	}
	
	public AvroTimeSeriesSchema(Schema.Type dataType) throws TimeSeriesException {
		resetSchema(dataType);
	}
	
	public Schema.Type getCurrentDataType() {
		return this.currentDataType;
	}
	
	public Schema getTimeSeriesSchema() {
		return this.timeSeriesSchema;
	}

	public Schema getBlockSchema() {
		return this.blockSchema;
	}

	public Schema getDataSchema() {
		return this.dataSchema;
	}
	
	public Schema getDataArraySchema() {
		return this.dataArraySchema;
	}
	
	public Schema getBlockArraySchema() {
		return this.blockArraySchema;
	}
	
	public void resetSchema(Schema.Type dataType) throws TimeSeriesException {
		if (!DynamicTypeTranslator.isSupportedDataType(dataType))
			throw new TimeSeriesException("Requested Data type is currently not supported " + dataType);
	
		this.currentDataType = dataType;
		List<Field> dataFields = new ArrayList<Field>();
		dataFields.add(new Schema.Field("offset", Schema.create(Schema.Type.INT), null, null));
		dataFields.add(new Schema.Field("val", Schema.create(this.currentDataType), null, null));  
		dataSchema = Schema.createRecord(dataFields);
		dataArraySchema = Schema.createArray(dataSchema);
		List<Field> blockFields = new ArrayList<Field>();
		
		blockFields.add(new Schema.Field("cluster", Schema.create(Schema.Type.STRING), null, null));
		blockFields.add(new Schema.Field("ln", Schema.create(Schema.Type.INT), null, null));
		blockFields.add(new Schema.Field("guid", Schema.create(Schema.Type.STRING), null, null));
		blockFields.add(new Schema.Field("startts", Schema.create(Schema.Type.LONG), null, null));
		blockFields.add(new Schema.Field("data", dataArraySchema, null, null));
		blockSchema = Schema.createRecord(blockFields);
		blockArraySchema = Schema.createArray(blockSchema);
		List<Field> timeSeriesFields = new ArrayList<Field>();
		timeSeriesFields.add(new Schema.Field("block", blockArraySchema, null, null));
		timeSeriesSchema = Schema.createRecord(timeSeriesFields);
		//System.out.println(timeSeriesSchema.toString(true));
	}
	
}
