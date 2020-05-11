// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.language.*;
import org.teiid.translator.*;
import org.teiid.metadata.*;
import org.apache.log4j.Logger;

import java.sql.*;

@Translator(name="squiggleeadapter", description="Translate time series data to and from a Cassandra Cluster")
public class TimeSeriesTranslator extends JDBCExecutionFactory {

	private static Logger logger = Logger.getLogger("com.squigglee.adapter.TimeSeriesTranslator");
	
	public TimeSeriesTranslator() 
	{
		super();
		logger.debug("In Squigglee time series cassandra translator C'Tor ");
	}

	@Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext
    		, RuntimeMetadata metadata, Connection conn)  		throws TranslatorException {
		
		logger.info("Creating result set execution for Squigglee time series cassandra translator");

		try {
			return new TimeSeriesQueryExecution(command, conn, executionContext, this);
			//return new JDBCQueryExecution(command, conn, executionContext, this);
		} catch (Exception e) {
			logger.error("Failed to create query execution class = ", e);
			throw new TranslatorException(e);
		}
    }
	
	@Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
    		throws TranslatorException {
    	logger.debug("Creating update execution for Squigglee time series cassandra translator");

    	try {
			return new TimeSeriesUpdateExecution(command, conn, executionContext, this);
			//return new JDBCUpdateExecution(command, conn, executionContext, this);
		} catch (Exception e) {
			logger.error("Failed to create handler class instance for class name = " );
			throw new TranslatorException(e);
		}
    }

	@Override
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return super.getSQLConversionVisitor();
    }

}
