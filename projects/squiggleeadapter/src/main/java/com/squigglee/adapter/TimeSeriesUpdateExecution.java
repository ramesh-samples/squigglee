// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.teiid.language.BatchedCommand;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Command;
import org.teiid.language.Insert;
//import org.teiid.query.parser.ParseException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.TranslatedCommand;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.api.restproxy.ConfigurationProxy;
import com.squigglee.api.restproxy.TimeSeriesProxy;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.serializers.avro.AvroTimeSeriesHandler;
import com.squigglee.core.interfaces.HandlerFactory;

@SuppressWarnings("unused")
public class TimeSeriesUpdateExecution extends JDBCUpdateExecution {
	private static Logger logger = Logger.getLogger("com.squigglee.adapter.TimeSeriesUpdateExecution");
	private int[] result;
	protected static int queryLimit = 1000;
	protected static IMasterDataHandler mdHandler = null;
	protected static TimeSeriesProxy tsProxy = null;
	protected static ConfigurationProxy configProxy = null;
	protected static String localCluster = null;
	protected static int localLn = 0;
	
	static {
		try {
			RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
			mdHandler = HandlerFactory.getMasterDataHandler();
			queryLimit = LocalNodeProperties.getSqlQueryLimit();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			localCluster = LocalNodeProperties.getClusterName();
			tsProxy = new TimeSeriesProxy(HandlerFactory.getDataHandler(), localLn, localCluster, queryLimit);
			configProxy = new ConfigurationProxy(HandlerFactory.getMasterDataHandler(), localLn, localCluster);
		} catch (TimeSeriesException e) {
			logger.error("Error initializing data handler", e);
		}
	}
	
	public TimeSeriesUpdateExecution(Command command, Connection connection, ExecutionContext context
			, JDBCExecutionFactory env) throws TranslatorException {
        super(command, connection, context, env);
        this.executionFactory.setCopyLobs(true);
		context.keepExecutionAlive(true);
    }
	
    @Override
    public void execute() throws TranslatorException {
    	logger.debug("Received Update:" + command);
    	System.out.println("Received Update:" + command);
		boolean succeeded = false;
		result = new int[0];
	    boolean commitType = getAutoCommit(null);
    	try {
            if (commitType) {
                connection.setAutoCommit(false);
            }
        	CommandParser cp = new CommandParser(command);
        	ParsedParameters parms = cp.getParsedParameters();
            result = processCommand(parms);
            succeeded = true;
    	} catch (Exception ex) {
    		logger.error("Error executing update statement(s),ex");
    		throw new TranslatorException(ex);
    	}
    	finally {
    		if (commitType) {
                restoreAutoCommit(!succeeded, null);
            }
    	}
    }
    
    private int[] processCommand(ParsedParameters parms) throws NumberFormatException, TimeSeriesException, IOException, SQLException {
    	int[] retVal = null;
    	switch (parms.getCommandType()) {
		case DELETE: retVal = processDeleteCommand(parms);
			break;
		case INSERT: retVal = processInsertCommand(parms);
			break;
		case SELECT: // not received in this update execution 
			break;
		case UPDATE: retVal = processUpdateCommand(parms);
			break;
		default:
			break;
    	}
    	return retVal;
    }
    
    private int[] processInsertCommand(ParsedParameters parms) throws NumberFormatException, TimeSeriesException, IOException, SQLException {
    	int[] retVal = new int[]{0};
    	switch(parms.getView()) {
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
			break;
		case PATTERNDOUBLES:
			break;
		case DOUBLES: {
			int count = insertData(parms.getInsertParameters());
			retVal[0] = count;
			break;
		}
		case MATCHEDFLOATS:
			break;
		case PATTERNFLOATS:
			break;
		case FLOATS:
			break;
		case MATCHEDINTEGERS:
			break;
		case PATTERNINTEGERS:
			break;
		case INTEGERS: {
			int count = insertData(parms.getInsertParameters());
			retVal[0] = count;
			break;
		}
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
    	return retVal;
    }
    
    private int[] processUpdateCommand(ParsedParameters parms) {
    	return null;
    }
    
    private int[] processDeleteCommand(ParsedParameters parms) throws TimeSeriesException {
    	int[] retVal = new int[]{0};
    	switch(parms.getView()) {
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
			break;
		case PATTERNDOUBLES:
			break;
		case DOUBLES: {
			break;
		}
		case MATCHEDFLOATS:
			break;
		case PATTERNFLOATS:
			break;
		case FLOATS:
			break;
		case MATCHEDINTEGERS:
			break;
		case PATTERNINTEGERS:
			break;
		case INTEGERS: {
			break;
		}
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
    	return retVal;
    }
    
    public int[] execute(BatchedUpdates batchedCommand) throws TranslatorException {
    	
        Command[] commands = batchedCommand.getUpdateCommands().toArray(new Command[batchedCommand.getUpdateCommands().size()]);
        int[] results = new int[commands.length];

        TranslatedCommand tCommand = null;
        
        try {
            List<TranslatedCommand> executedCmds = new ArrayList<TranslatedCommand>();
            TranslatedCommand previousCommand = null;
            
            for (int i = 0; i < commands.length; i++) {
            	tCommand = translateCommand(commands[i]);
                if (tCommand.isPrepared()) {
                    PreparedStatement pstmt = null;
                    if (previousCommand != null && previousCommand.isPrepared() && previousCommand.getSql().equals(tCommand.getSql())) {
                        pstmt = (PreparedStatement)statement;
                    } else {
                        if (!executedCmds.isEmpty()) {
                            executeBatch(i, results, executedCmds);
                        }
                        pstmt = getPreparedStatement(tCommand.getSql());
                    }
                    bind(pstmt, tCommand.getPreparedValues(), null);
                    pstmt.addBatch();
                } else {
                    if (previousCommand != null && previousCommand.isPrepared()) {
                        executeBatch(i, results, executedCmds);
                        getStatement();
                    }
                    if (statement == null) {
                        getStatement();
                    }
                    statement.addBatch(tCommand.getSql());
                }
                executedCmds.add(tCommand);
                previousCommand = tCommand;
            }
            if (!executedCmds.isEmpty()) {
                executeBatch(commands.length, results, executedCmds);
            }
        } catch (SQLException e) {
        	logger.error("Found exception executing batched updates ",e);
            throw new TranslatorException(e);
        }
        return results;
    }

    private void executeBatch(int commandCount,
                              int[] results,
                              List<TranslatedCommand> commands) throws SQLException {
        int[] batchResults = statement.executeBatch();
        addStatementWarnings();
        for (int j = 0; j < batchResults.length; j++) {
            results[commandCount - 1 - j] = batchResults[batchResults.length - 1 - j];
        }
        commands.clear();
    }

    /**
     * @param translatedComm
     * @throws TranslatorException
     * @since 4.3
     */
	private int[] executeTranslatedCommand(TranslatedCommand translatedComm) throws TranslatorException {
        String sql = translatedComm.getSql();
        try {
        	int updateCount = 0;
            if (!translatedComm.isPrepared()) {
                updateCount = getStatement().executeUpdate(sql);
                addStatementWarnings();
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
            	
            	Iterator<? extends List<?>> vi = null;
            	if (command instanceof BatchedCommand) {
            		BatchedCommand batchCommand = (BatchedCommand)command;
            		vi = batchCommand.getParameterValues();
            	}
            	
                if (vi != null) {
            		int maxBatchSize = (command instanceof Insert)?this.executionFactory.getMaxPreparedInsertBatchSize():Integer.MAX_VALUE;
            		boolean done = false;
            		outer: while (!done) {
            			for (int i = 0; i < maxBatchSize; i++) {
            				if (vi.hasNext()) {
    	            			List<?> values = vi.next();
    	            			bind(pstatement, translatedComm.getPreparedValues(), values);
            				} else {
            					if (i == 0) {
	            					break outer;
	            				}
	            				done = true;
	            				break;
            				}
            			}
            		    int[] results = pstatement.executeBatch();
            		    
            		    for (int i=0; i<results.length; i++) {
            		        updateCount += results[i];
            		    }
            		}
                } else {
                	bind(pstatement, translatedComm.getPreparedValues(), null);
        			updateCount = pstatement.executeUpdate();
        			addStatementWarnings();
                }
            } 
            return new int[] {updateCount};
        } catch (SQLException err) {
        	logger.error("Found exception executing translated command during update ",err);
        	throw new TranslatorException(err);
        }
    }

    /**
     * @param command
     * @return
     * @throws TranslatorException
     */
    private boolean getAutoCommit(TranslatedCommand tCommand) throws TranslatorException {
    	try {
            return connection.getAutoCommit();
        } catch (SQLException err) {
        	logger.error("Found exception getting connection auto commit status ",err);
        	throw new TranslatorException(err);
        }
    }

    /**
     * If the auto comm
     * 
     * @param exceptionOccurred
     * @param command
     * @throws TranslatorException
     */
    private void restoreAutoCommit(boolean exceptionOccurred,
                                   TranslatedCommand tCommand) throws TranslatorException {
        try {
            if (exceptionOccurred) {
                connection.rollback();
                logger.warn("Rolled back update since exception occurred");
            }
        } catch (SQLException err) {
        	logger.error("Found exception rolling back transaction during update processing",err);
            throw new TranslatorException(err);
        } finally {
        	try {
        		connection.commit(); // in JbossAs setAutocommit = true does not trigger the commit.
        		connection.setAutoCommit(true);
        	} catch (SQLException err) {
        		logger.error("Found exception commiting transaction during update processing",err);
                throw new TranslatorException(err);
            }
        }
    }
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
    		TranslatorException {
    	return result;
    }
 
    public static void setPreparedStatementValue(PreparedStatement ps, int index, Object value, int valueTypeInt) throws SQLException {
		switch (valueTypeInt) {
			case Types.CLOB: 	if (value != null) ps.setClob(index, (Clob) value); else ps.setNull(index, Types.CLOB);
								break;
			case Types.VARCHAR: if (value != null)  ps.setString(index, value.toString()); else ps.setNull(index, Types.VARCHAR);
								break;
			case Types.INTEGER: if (value != null) ps.setInt(index,  ((Integer) value ).intValue() ); else ps.setNull(index, Types.INTEGER);
								break;
			case Types.BIGINT: if (value != null) ps.setLong(index,  ((Long) value ).longValue() ); else ps.setNull(index, Types.BIGINT);
								break;
			case Types.TIMESTAMP: if (value != null) ps.setTimestamp(index,  new Timestamp(new java.util.Date().getTime())); else ps.setNull(index, Types.TIMESTAMP);
								break;
			case Types.BOOLEAN: if (value != null)  ps.setBoolean(index, (Boolean) value); else ps.setNull(index, Types.BOOLEAN);
								break;
			default: 			if (value != null) ps.setString(index, value.toString()); else ps.setNull(index, Types.NVARCHAR);
			 					break;
		}
    }

    private static int insertData(List<Map<String, Object>> insertParameters) throws TimeSeriesException {
    	System.out.println("Insert Parameters = " + insertParameters);
		
		SortedMap<Long, Object> data = new TreeMap<Long, Object>();
		String clust = insertParameters.get(0).get("clust").toString();
		int ln = Integer.parseInt(insertParameters.get(0).get("ln").toString());
		String id = insertParameters.get(0).get("id").toString();
		long tsmillisstart = ((java.sql.Timestamp) insertParameters.get(0).get("ts")).getTime();
		long tsmillisend = ((java.sql.Timestamp) insertParameters.get(insertParameters.size()-1).get("ts")).getTime();
		long offsetstart = Long.parseLong(insertParameters.get(0).get("off").toString());
		long offsetend = Long.parseLong(insertParameters.get(insertParameters.size()-1).get("off").toString());
		
    	Map<Long, SortedMap<Long, Object>> dataMap = new HashMap<Long, SortedMap<Long, Object>>();
		List<MasterData> mdList = mdHandler.getMasterData(clust, ln, id, tsmillisstart, tsmillisend);
		if (mdList == null || mdList.isEmpty())
			return 0;
		for (MasterData m : mdList)
			if (!dataMap.containsKey(m.getStartts()))
				dataMap.put(m.getStartts(), new TreeMap<Long, Object>());
		for (int j = 0; j < insertParameters.size(); j++) {
			long tsmillis = ((java.sql.Timestamp) insertParameters.get(0).get("ts")).getTime();
			MasterData md = null;
			for (MasterData m : mdList)
				if (m.getStartts() <= tsmillis) {
					md = m;
					break;
				}
			if (md == null)
				continue;
			long offset = TimeSeriesShard.getOffset(md.getFreq(), tsmillis, Long.parseLong(insertParameters.get(0).get("off").toString()));
			dataMap.get(md.getStartts()).put(offset, insertParameters.get(j).get("val"));
		}
		int count = 0;
		for (long startts : dataMap.keySet()) {
			count += dataMap.get(startts).size();
			tsProxy.putData(clust, ln, id, tsmillisstart, (int) offsetstart, tsmillisend, (int) offsetend, dataMap.get(startts));
		}
		return count;
    }
}
