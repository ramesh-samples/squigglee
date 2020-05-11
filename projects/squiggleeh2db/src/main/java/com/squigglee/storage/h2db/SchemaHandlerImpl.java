// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.storage.h2db;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.squigglee.core.config.MasterData;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.TimeSeriesException;

public class SchemaHandlerImpl extends HandlerBase implements ISchemaHandler {
	private static Logger logger = Logger.getLogger("com.squigglee.storage.h2db.SchemaHandlerImpl");
	protected String createTableString = "CREATE TABLE IF NOT EXISTS %s " + "(off bigint, val %s, CONSTRAINT %s PRIMARY KEY (off));";
	protected String createIndexString = "CREATE INDEX %s ON %s (off desc); ";
	protected String dropTableString = "DROP TABLE IF EXISTS %s;";

	@Override
	public boolean createSchema(MasterData md) throws TimeSeriesException {
		String dataType = md.getDatatype();
		Connection conn = getConnection();
    	String ddl = String.format(createTableString, md.getKs() + "_" + md.getId(), dataType, "PK_" + md.getKs() + "_" + md.getId());
    	String ddlidx = String.format(createIndexString, "IDX_" + md.getKs() + "_" + md.getId(), md.getKs() + "_" + md.getId());
    	try {
			boolean result = conn.createStatement().execute(ddl);
			boolean result1 = conn.createStatement().execute(ddlidx);
			if (conn != null && !conn.isClosed())
				conn.close();
			return result && result1;
		} catch (SQLException e) {
			logger.error("Error executing DDL = " + ddl,e);
		}
		return false;
	}
	
	@Override
	public boolean createSchema(MasterData md, int offsetCount) throws TimeSeriesException {
		return createSchema(md);
	}

	@Override
	public void deleteSchema(long id) throws TimeSeriesException {
		//TODO RKR implement this
		//if (md == null)
		//	return;
		//Connection conn = getConnection();
    	//String ddl = String.format(dropTableString, md.getKs() + "_" + md.getId());
    	//try {
			//conn.createStatement().execute(ddl);
			//if (conn != null && !conn.isClosed())
			//	conn.close();
		//} catch (SQLException e) {
			//logger.error("Error executing DDL = " + ddl,e);
		//}
	}
	
	@Override
	public boolean deletePatternIndexTables(List<MasterData> list, String index) throws TimeSeriesException {
		boolean result = false;
		try {
			for (MasterData md : list) {
				for (String ext : new String[]{"",".p",".t"}) {
					File file = new File(storagePath + "/" + index + "_" + md.getId() + ext);
					if (file.exists()) {
						file.delete();
						result = true;
					}
				}
				System.out.println("Deleted index tables for index = " + index + "_" + md.getId());
				logger.debug("Deleted index tables for index = " + index + "_" + md.getId());
			}
		} catch (Exception e) {
			System.out.println("Failed to delete index tables for index = " + index);
			logger.debug("Failed to delete index tables for index = " + index, e);
		}
		
		return result;
	}
	
	@Override
	public boolean createPatternIndexTables(long id, String idxTableName, String dataKeyspace) throws TimeSeriesException {
		boolean result = false;
		if (idxTableName.toLowerCase().contains("ptrn")) {
			Connection conn = getConnection();
	    	try {
				//String create = "CREATE TABLE IF NOT EXISTS " + idxTableName + "_" + id + " (hashes char(64), val bigint, primary key (hashes,val));";
				String create = "CREATE TABLE IF NOT EXISTS " + idxTableName + "_" + id + " (hashes int primary key, vals varchar(2048));";
				conn.createStatement().execute(create);
				result = true;
				//if (result != null && result.length > 0) {
					System.out.println("Created the index table for index = " + idxTableName + "_" + id);
					logger.debug("Created the index table for index = " + idxTableName + "_" + id);
				//}
				if (conn != null && !conn.isClosed())
					conn.close();
			} catch (SQLException e) {
				System.out.println("Failed to create index tables for index = " + idxTableName + "_" + id);
				logger.debug("Failed to create index tables for index = " + idxTableName + "_" + id);
			}
		}
		return result;
	}
}
