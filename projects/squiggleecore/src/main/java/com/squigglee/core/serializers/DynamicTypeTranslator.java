// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.serializers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

//import com.datastax.driver.core.Row;



import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;
import com.squigglee.core.entity.ViewType;

public class DynamicTypeTranslator {
	
	public static List<Schema.Type> supportedTypesList = null;
	
	static {
		supportedTypesList = new ArrayList<Schema.Type>();
		supportedTypesList.add(Schema.Type.DOUBLE);
		supportedTypesList.add(Schema.Type.BYTES);
		supportedTypesList.add(Schema.Type.BOOLEAN);
		supportedTypesList.add(Schema.Type.FLOAT);
		supportedTypesList.add(Schema.Type.INT);
		supportedTypesList.add(Schema.Type.LONG);
		supportedTypesList.add(Schema.Type.STRING);
	}
	
	public static boolean isSupportedDataType(Schema.Type dataType) {
		if (supportedTypesList.contains(dataType))
			return true;
		return false;
	}
	
	/*
	// from AVRO to cql result set via Datastax 
	public static Object getDataVal(Row dataRow, Schema.Type dataType) throws TimeSeriesException {
		switch(dataType) {
			case DOUBLE: 
				return new Double(dataRow.getDouble("val"));
			case BOOLEAN: 
				return new Boolean(dataRow.getBool("val"));
			case FLOAT: 
				return new Float(dataRow.getFloat("val"));
			case INT: 
				return new Integer(dataRow.getInt("val"));
			case LONG: 
				return new Long(dataRow.getLong("val"));				
			case STRING: 
				return dataRow.getString("val");
			case BYTES: 
				return dataRow.getBytes("val");
			default: 
				throw new TimeSeriesException("Requested data type is currently not supported " + dataType);
		}
	}
	*/
	
	
	// from CQL 3 to AVRO 
	public static Schema.Type getSchemaType(String dataType) throws TimeSeriesException {
		if (dataType.equalsIgnoreCase("int"))
			return Schema.Type.INT;
		if (dataType.equalsIgnoreCase("double"))
			return Schema.Type.DOUBLE;
		if (dataType.equalsIgnoreCase("boolean"))
			return Schema.Type.BOOLEAN;
		if (dataType.equalsIgnoreCase("float"))
			return Schema.Type.FLOAT;
		if (dataType.equalsIgnoreCase("long"))
			return Schema.Type.LONG;
		if (dataType.equalsIgnoreCase("string"))
			return Schema.Type.STRING;
		if (dataType.equalsIgnoreCase("text"))
			return Schema.Type.STRING;		
		if (dataType.equalsIgnoreCase("blob"))
			return Schema.Type.BYTES;

		throw new TimeSeriesException("Requested master data type is currently not support; " + dataType);
	}
	
	
	// from AVRO to cql data type 
	public static String getCQLDataType(Schema.Type dataType) throws TimeSeriesException {
		switch(dataType) {
			case DOUBLE: 
				return "double";
			case BOOLEAN: 
				return "boolean";
			case FLOAT: 
				return "float";
			case INT: 
				return "int";
			case LONG: 
				return "bigint";				
			case STRING: 
				return "varchar";
			case BYTES: 
				return "blob";
			default: 
				throw new TimeSeriesException("Requested data type is currently not supported " + dataType);
		}
	}
	
	// from AVRO to cql data type 
	public static Object parseStringObject(String value, Schema.Type dataType) throws TimeSeriesException {
		switch(dataType) {
			case DOUBLE: 
				return new Double(Double.parseDouble(value));
			case BOOLEAN: 
				return new Boolean(Boolean.parseBoolean(value));
			case FLOAT: 
				return new Float(Float.parseFloat(value));
			case INT: 
				return new Integer(Integer.parseInt(value));
			case LONG: 
				return new Long(Long.parseLong(value));
			case STRING: 
				return value;
			case BYTES: 
				throw new TimeSeriesException("Requested data type is currently not supported " + dataType);
			default: 
				throw new TimeSeriesException("Requested data type is currently not supported " + dataType);
		}
	}
	
	public static Object convertDoubleToNumeric(double val, String dataType) throws TimeSeriesException {
		switch(getSchemaType(dataType)) {
		case DOUBLE: 
			return val;
		case FLOAT: 
			return new Float( (float) val);
		case INT: 
			return (new Double(Math.round(val))).intValue();
		case LONG: 
			return (new Double(Math.round(val))).longValue();
		default: 
			throw new TimeSeriesException("Requested data type is currently not supported " + dataType);
		}
	}
	
	// from AVRO to cql data type 
	public static Object parseStringObject(String value, String dataType) throws TimeSeriesException {
		return parseStringObject(value, getSchemaType(dataType));
	}
	
	public static Schema.Type getRuntimeType(Object obj) throws TimeSeriesException {
		if (obj instanceof Double)
			return Schema.Type.DOUBLE;
		if (obj instanceof Boolean)
			return Schema.Type.BOOLEAN;
		if (obj instanceof Float)
			return Schema.Type.FLOAT;
		if (obj instanceof Integer)
			return Schema.Type.INT;
		if (obj instanceof Long)
			return Schema.Type.LONG;
		if (obj instanceof String)
			return Schema.Type.STRING;
		if (obj instanceof byte[])
			return Schema.Type.BYTES;

		throw new TimeSeriesException("Requested master data type is currently not support; " + obj);		
	}

	public static void setPreparedStatementValue(int index, PreparedStatement ps, Object val, Schema.Type dataType) throws SQLException {
		switch(dataType) {
			case DOUBLE:
				ps.setDouble(index, (Double) val);
				break;
			case BOOLEAN:
				ps.setBoolean(index, (Boolean) val);
				break;
			case FLOAT:
				ps.setFloat(index, (Float) val);
				break;
			case INT:
				ps.setInt(index, (Integer) val);
				break;
			case LONG:
				ps.setLong(index, (Long) val);
				break;
			case STRING:
				ps.setString(index, val.toString());
				break;
			case BYTES:
				ps.setBytes(index, (byte[]) val);
				break;
		default:
			break;				
		}
	}

	//for now Cassandra specific data types only 
	
	// from API view to cql data type
	public static String getViewDataType(View view) {
		switch (view) {
			case MATCHEDBIGDECIMALS: return "text";
			case PATTERNBIGDECIMALS: return "text";
			case BIGDECIMALS: return "text";
			case MATCHEDBIGINTEGERS: return "text";
			case PATTERNBIGINTEGERS: return "text";
			case BIGINTEGERS: return "text";
			case MATCHEDBLOBS: return "blob";
			case PATTERNBLOBS: return "blob";
			case BLOBS: return "blob";
			case MATCHEDBOOLEANS: return "blob";
			case PATTERNBOOLEANS: return "boolean";
			case BOOLEANS: return "boolean";
			case BULKDATA: return "blob";
			case MATCHEDCLOBS: return "text";
			case PATTERNCLOBS: return "text";
			case CLOBS: return "text";
			case MATCHEDDOUBLES: return "double";
			case PATTERNDOUBLES: return "double";
			case DOUBLES: return "double";
			case MATCHEDFLOATS: return "float";
			case PATTERNFLOATS: return "float";
			case FLOATS: return "float";
			case MATCHEDINTEGERS: return "int";
			case PATTERNINTEGERS: return "int";
			case INTEGERS: return "int";
			case MATCHEDLONGS: return "bigint";
			case PATTERNLONGS: return "bigint";
			case LONGS: return "bigint";
			case MASTERDATA: return null;
			case SAMPLEDBIGDECIMALS: return "text";
			case SAMPLEDBIGINTEGERS: return "text";
			case SAMPLEDBLOBS: return "blob";
			case SAMPLEDBOOLEANS: return "boolean";
			case SAMPLEDCLOBS: return "text";
			case SAMPLEDDOUBLES: return "double";
			case SAMPLEDFLOATS: return "float";
			case SAMPLEDINTEGERS: return "int";
			case SAMPLEDLONGS: return "bigint";
			case SAMPLEDSTRINGS: return "text";
			case SKETCHEDBIGDECIMALS: return "text";
			case SKETCHEDBIGINTEGERS: return "text";
			case SKETCHEDBLOBS: return "blob";
			case SKETCHEDBOOLEANS: return "boolean";
			case SKETCHEDCLOBS: return "text";
			case SKETCHEDDOUBLES: return "double";
			case SKETCHEDFLOATS: return "float";
			case SKETCHEDINTEGERS: return "int";
			case SKETCHEDLONGS: return "bigint";
			case SKETCHEDSTRINGS: return "text";
			case MATCHEDSTRINGS: return "text";
			case PATTERNSTRINGS: return "text";
			case STRINGS: return "text";
			default: return null;
		}
	}

	// from API view to cql data type
	public static String getInMemDataType(View view) {
		switch (view) {
			case MATCHEDBIGDECIMALS: return "varchar(256)";
			case PATTERNBIGDECIMALS: return "varchar(256)";
			case BIGDECIMALS: return "varchar(256)";
			case MATCHEDBIGINTEGERS: return "varchar(256)";
			case PATTERNBIGINTEGERS: return "varchar(256)";
			case BIGINTEGERS: return "varchar(256)";
			case MATCHEDBLOBS: return "blob";
			case PATTERNBLOBS: return "blob";
			case BLOBS: return "blob";
			case MATCHEDBOOLEANS: return "blob";
			case PATTERNBOOLEANS: return "boolean";
			case BOOLEANS: return "boolean";
			case BULKDATA: return "blob";
			case MATCHEDCLOBS: return "clob";
			case PATTERNCLOBS: return "clob";
			case CLOBS: return "clob";
			case MATCHEDDOUBLES: return "double";
			case PATTERNDOUBLES: return "double";
			case DOUBLES: return "double";
			case MATCHEDFLOATS: return "float";
			case PATTERNFLOATS: return "float";
			case FLOATS: return "float";
			case MATCHEDINTEGERS: return "integer";
			case PATTERNINTEGERS: return "integer";
			case INTEGERS: return "integer";
			case MATCHEDLONGS: return "bigint";
			case PATTERNLONGS: return "bigint";
			case LONGS: return "bigint";
			case MASTERDATA: return null;
			case SAMPLEDBIGDECIMALS: return "varchar(256)";
			case SAMPLEDBIGINTEGERS: return "varchar(256)";
			case SAMPLEDBLOBS: return "blob";
			case SAMPLEDBOOLEANS: return "boolean";
			case SAMPLEDCLOBS: return "clob";
			case SAMPLEDDOUBLES: return "double";
			case SAMPLEDFLOATS: return "float";
			case SAMPLEDINTEGERS: return "integer";
			case SAMPLEDLONGS: return "bigint";
			case SAMPLEDSTRINGS: return "varchar(256)";
			case SKETCHEDBIGDECIMALS: return "varchar(256)";
			case SKETCHEDBIGINTEGERS: return "varchar(256)";
			case SKETCHEDBLOBS: return "blob";
			case SKETCHEDBOOLEANS: return "boolean";
			case SKETCHEDCLOBS: return "clob";
			case SKETCHEDDOUBLES: return "double";
			case SKETCHEDFLOATS: return "float";
			case SKETCHEDINTEGERS: return "integer";
			case SKETCHEDLONGS: return "bigint";
			case SKETCHEDSTRINGS: return "varchar(256)";
			case MATCHEDSTRINGS: return "varchar(256)";
			case PATTERNSTRINGS: return "varchar(256)";
			case STRINGS: return "varchar(256)";
			default: return null;
		}
	}
	
	// from CQL 3 to View given the type of view 
	public static View getViewName(String dataType, ViewType viewType) throws TimeSeriesException {
		
		String name = null;	
		if (dataType.equalsIgnoreCase("double"))
			name = "DOUBLES";
		if (dataType.equalsIgnoreCase("boolean"))
			name =  "BOOLEANS";
		if (dataType.equalsIgnoreCase("float"))
			name = "FLOATS";
		if (dataType.equalsIgnoreCase("int"))
			name = "INTEGERS";
		if (dataType.equalsIgnoreCase("long"))
			name = "LONGS";
		if (dataType.equalsIgnoreCase("string"))
			name = "STRINGS";
		if (dataType.equalsIgnoreCase("text"))
			name = "CLOBS";		
		if (dataType.equalsIgnoreCase("blob"))
			name = "BLOBS";
		switch(viewType) {
			case DATA:
				return Enum.valueOf(View.class, name);
			case MATCH:
				return Enum.valueOf(View.class, "MATCHED" + name);
			case PATTERN:
				return Enum.valueOf(View.class, "PATTERN" + name);
			case SAMPLE:
				return Enum.valueOf(View.class, "SAMPLED" + name);
			case SKETCH:
				return Enum.valueOf(View.class, "SKETCHED" + name);
			default:
				break;
		}
		throw new TimeSeriesException("Requested data type is currently not supported; " + dataType + " for view type = " + viewType);
	}
}
