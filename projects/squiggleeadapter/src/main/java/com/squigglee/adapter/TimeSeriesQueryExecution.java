// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.jdbc.JDBCQueryExecution;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.squigglee.api.rest.RESTFactory;
import com.squigglee.api.restproxy.ConfigurationProxy;
import com.squigglee.api.restproxy.TimeSeriesProxy;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TimeSeriesShard;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.serializers.DynamicTypeTranslator;
import com.squigglee.core.serializers.avro.AvroTimeSeriesDeserializer;
import com.squigglee.core.interfaces.HandlerFactory;

public class TimeSeriesQueryExecution extends JDBCQueryExecution {
	
	private static Logger logger = Logger.getLogger("com.squigglee.adapter.TimeSeriesQueryExecution");
	protected static int queryLimit = 1000;
	protected static IMasterDataHandler mdHandler = null;
	protected static TimeSeriesProxy tsProxy = null;
	protected static ConfigurationProxy configProxy = null;
	protected static String localCluster = null;
	protected static int localLn = 0;
	static {
		try {
			RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
			queryLimit = LocalNodeProperties.getSqlQueryLimit();
			mdHandler = HandlerFactory.getMasterDataHandler();
			localLn = LocalNodeProperties.getNodeLogicalNumber();
			localCluster = LocalNodeProperties.getClusterName();
			tsProxy = new TimeSeriesProxy(HandlerFactory.getDataHandler(), localLn, localCluster, queryLimit);
			configProxy = new ConfigurationProxy(HandlerFactory.getMasterDataHandler(), localLn, localCluster);
		} catch (TimeSeriesException e) {
			queryLimit = 1000;
		}
	}
	
	public TimeSeriesQueryExecution(Command command, Connection connection, ExecutionContext context
			, JDBCExecutionFactory env) throws TranslatorException {
		super(command, connection, context, env);
		logger.debug(" in query execution constructor");
		this.executionFactory.setCopyLobs(true);
		context.keepExecutionAlive(true);
	}
	
    @Override
    public void execute() throws TranslatorException {
    	logger.debug("Received Query:" + command + " at time = " + DateTime.now());
    	System.out.println("Received Query:" + command + " at time = " + DateTime.now());
    	columnDataTypes = ((QueryExpression)command).getColumnTypes();
        try {
        	//general pattern for handling queries
        	// parse query, delete from in-memory table, fetch from source, insert into in-memory table, execute original query 
        	CommandParser cp = new CommandParser(command);
        	ParsedParameters parms = cp.getParsedParameters();
        	processQuery(parms);
        	TranslatedCommand translatedComm = translateCommand(command);
        	String sql = translatedComm.getSql();
        	if (!translatedComm.isPrepared()) {
                results = getStatement().executeQuery(sql);
            } else {
            	PreparedStatement pstatement = getPreparedStatement(sql);
                bind(pstatement, translatedComm.getPreparedValues(), null);
                results = pstatement.executeQuery();
            }
		} catch (Exception e) {
			logger.error("Found error executing query = " + command,e);
			throw new TranslatorException(e);
		}
    }
    
    private void processQuery(ParsedParameters parms) throws SQLException, TimeSeriesException {
    	connection.createStatement().executeUpdate("delete from TIMESERIES." + parms.getView().name() + ";");
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
			//setBulkData(parms);
			break;
		case MATCHEDCLOBS:
			break;
		case PATTERNCLOBS:
			break;
		case CLOBS:
			break;
		case MATCHEDDOUBLES:
			//setMatches(parms);
			break;
		case PATTERNDOUBLES:
			//setPatternData(parms);
			break;
		case DOUBLES:
			setData(parms);
			break;
		case MATCHEDFLOATS:
			break;
		case PATTERNFLOATS:
			break;
		case FLOATS:
			break;
		case MATCHEDINTEGERS:
			//setMatches(parms);
			break;
		case PATTERNINTEGERS:
			break;
		case INTEGERS:
			setData(parms);
			break;
		case MATCHEDLONGS:
			break;
		case PATTERNLONGS:
			break;
		case LONGS:
			break;
		case MASTERDATA:
			setMasterData(parms);
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
			//setSampledData(parms);
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
			//setSketch(parms);
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
    }
    
/*    protected void setBulkData(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				String id = null, cluster = null;
				int ln = -1, startoffset = 0, endoffset = 0;
				java.sql.Timestamp start = null, end = null;
				byte[] bdata = null;
				
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("cluster")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("startdt")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							start = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("="));
						}
					}
					if (colName.equalsIgnoreCase("startoffset")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							startoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("enddt")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							end = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("="));
						}
					}
					if (colName.equalsIgnoreCase("endoffset")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							endoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
				}
				IDataHandler dataHandler = HandlerFactory.getDataHandler();
				Object[] blockQueryResults = dataHandler.readBlockData(cluster, ln, id, start.getTime(), startoffset, end.getTime(), endoffset);
				dataHandler.shutdown();
				bdata = (byte[]) blockQueryResults[2];
				PreparedStatement ps = connection.prepareStatement
		    			("INSERT INTO TIMESERIES." + parms.getView().name() + "(ln, id, startdt, startoffset, enddt, endoffset, datatype, bdata) VALUES (?,?,?,?,?,?,?,?);");
				String datatype = DynamicTypeTranslator.getViewDataType(parms.getView());
				Schema.Type dataType = DynamicTypeTranslator.getSchemaType(datatype);
				
				ps.setInt(1, ln);
				ps.setString(2, id);
				ps.setTimestamp(3, start);
				ps.setInt(4, startoffset);
				ps.setTimestamp(5, end);
				ps.setInt(6, endoffset);
				ps.setString(7, blockQueryResults[0].toString());
				DynamicTypeTranslator.setPreparedStatementValue(8, ps, bdata, dataType);
				ps.execute();
			}
    	}
    }*/
    
/*    protected void setSketch(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				String id = null, st = null, cluster = null;
				int ln = -1;
				//double freq = -1.0;
				Object startVal = null, endVal = null;
				
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("cluster")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					
					if (colName.equalsIgnoreCase("st")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							st = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					
					if (colName.equalsIgnoreCase("val")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							startVal = parms.getWhereColumns().get(tableName).get(colName).get("=");
							endVal = parms.getWhereColumns().get(tableName).get(colName).get("=");
						}
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">"))
							startVal = parms.getWhereColumns().get(tableName).get(colName).get(">");
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">="))
							startVal = parms.getWhereColumns().get(tableName).get(colName).get(">=");
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<"))
							endVal = parms.getWhereColumns().get(tableName).get(colName).get("<");
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<="))
							endVal = parms.getWhereColumns().get(tableName).get(colName).get("<=");		
					}
				}
				ISketchHandler skchHandler = HandlerFactory.getSketchHandler();
				skchHandler.reset(DynamicTypeTranslator.getViewDataType(parms.getView()));
				Stats stats = skchHandler.statistics(cluster, ln, id);
				if (stats == null)
					return;
				long count = stats.getCount();
				double pointFreq = -1.0;
				if (startVal == endVal)
					pointFreq = skchHandler.pointQuery(cluster, ln, id, startVal) * 1.0 / count;
				else
					pointFreq = skchHandler.rangeQuery(cluster, ln, id, startVal, endVal) * 1.0 / count;
				skchHandler.shutdown();
				PreparedStatement ps = connection.prepareStatement
		    			("INSERT INTO TIMESERIES." + parms.getView().name() + "(ln, id, freq, val, st) VALUES (?,?,?,?,?);");
				Schema.Type dataType = DynamicTypeTranslator.getSchemaType(DynamicTypeTranslator.getViewDataType(parms.getView()));
				ps.setInt(1, ln);
				ps.setString(2, id);
				ps.setDouble(3, pointFreq);
				DynamicTypeTranslator.setPreparedStatementValue(4, ps, startVal, dataType);
				ps.setString(5, st);
				ps.execute();
			}
    	}
    }*/
    
/*    protected void setMatches(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				String id = null,pid = null, cluster = null;
				int ln = -1, rank = -1;
				//long startts = -1, endts = -1;
				double radius = -1.0;
				
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("cluster")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("pid")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							pid = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("rank")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<=")) {
							rank = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("<=").toString());
						}
					}
					if (colName.equalsIgnoreCase("radius")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							radius = Double.parseDouble(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					
					if (colName.equalsIgnoreCase("ts")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
						}
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">"))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">")).getTime() + 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">="))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">=")).getTime();
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<"))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<")).getTime() - 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<="))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<=")).getTime();		
					}
					
				}
				
				IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
				List<TimeSeriesConfig> list = mdHandler.getMasterData(cluster, ln,id);
				IPatternHandler ptrnHandler = HandlerFactory.getPatternHandler();
				ptrnHandler.reset(DynamicTypeTranslator.getViewDataType(parms.getView()));

				byte[] matches = null;
				//byte[] matches = ptrnHandler.fetchMatches(cluster, list, pid, parms.getView(), rank, radius);
				ptrnHandler.shutdown();
				mdHandler.shutdown();
				PreparedStatement ps = connection.prepareStatement("INSERT INTO TIMESERIES." + parms.getView().name() +
						" (ln, id, ts, val, pid, rank, radius) VALUES (?,?,?,?,?,?,?);");
				
				AvroTimeSeriesDeserializer atsd = new AvroTimeSeriesDeserializer();
				Schema.Type dataType = DynamicTypeTranslator.getSchemaType(DynamicTypeTranslator.getViewDataType(parms.getView()));
				atsd.resetSchema(dataType);
				if (matches == null)
					return;
				atsd.setRawData(matches);
				int numOfBlocks = atsd.getBlockCount();
				for (int i=0; i< numOfBlocks; i++) {
					int dataCount = atsd.getDataCount(i);
					long stts = atsd.getStartts(i);
					String id1 = atsd.getGuid(i);
					int ln1 = atsd.getLn(i);
					for (int j=0; j< dataCount; j++) {
						ps.setInt(1, ln1);
						ps.setString(2,id1);
						ps.setTimestamp(3, new java.sql.Timestamp(stts + atsd.getOffset(i, j)));
						DynamicTypeTranslator.setPreparedStatementValue(4, ps, atsd.getVal(i, j), dataType);
						ps.setString(5, pid);
						ps.setInt(6,(i+1));
						ps.setDouble(7, radius);
						ps.addBatch();
					}
				}
				ps.executeBatch();
			}
    	}
    }*/
    
    protected void setMasterData(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	IMasterDataHandler mdHandler = HandlerFactory.getMasterDataHandler();
    	List<TimeSeriesConfig> configs = null;
    	if (parms == null || parms.getWhereColumns() == null || parms.getWhereColumns().keySet().isEmpty())
    		configs = mdHandler.getGlobalConfig();
    	String id = null, cluster = null;
		int ln = -1;
		for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("clust")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
				}
			}
		}
		if (cluster == null)
			configs = mdHandler.getGlobalConfig();
		else if (ln == -1)
			configs = mdHandler.getClusterConfig(cluster);
		else if (id == null)
			configs = mdHandler.getNodeMasterData(cluster, ln);
		else
			configs = mdHandler.getMasterData(cluster, ln, id);
		configs = TimeSeriesConfig.collapseTimeIntervals(configs);
		mdHandler.shutdown();
		PreparedStatement statement = connection.prepareStatement("INSERT INTO TIMESERIES.MASTERDATA (clust, ln, id, freq, datatype, indexes, startts, endts) VALUES (?,?,?,?,?,?,?,?);");
		for (TimeSeriesConfig config : configs) {
			statement.setString(1, config.getCluster());
			statement.setInt(2, config.getLogicalNode());
			statement.setString(3, config.getGuid());
			statement.setString(4, config.getFrequency().toString());
			statement.setString(5, config.getDatatype());
			statement.setString(6, config.getIndexes());
			statement.setTimestamp(7, new java.sql.Timestamp(config.getStartDate().getMillis()));
			statement.setTimestamp(8, new java.sql.Timestamp(config.getEndDate().getMillis()));
			statement.addBatch();
		}
		statement.executeBatch();
   }
      
/*    protected void setPatternData(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				String cluster = null;
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("cluster")) {
						cluster = parms.getWhereColumns()
								.get(tableName).get(colName).get("=").toString();
						break;
					}
				}
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("pguid")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							String pguid = parms.getWhereColumns()
									.get(tableName).get(colName).get("=").toString();
							IPatternHandler ptrnHandler = HandlerFactory.getPatternHandler();
							List<String> vals = ptrnHandler.fetchPattern(cluster, pguid);
							ptrnHandler.shutdown();
							PreparedStatement ps = connection.prepareStatement
					    			("INSERT INTO TIMESERIES." + parms.getView().name() + "(pguid,pindx,val) VALUES (?,?,?);");
							Schema.Type dataType = DynamicTypeTranslator.getSchemaType(DynamicTypeTranslator.getViewDataType(parms.getView()));
							for (int i = 0; i < vals.size(); i++) {
								ps.setString(1, pguid);
								ps.setInt(2, i);
								DynamicTypeTranslator.setPreparedStatementValue(3, ps, vals.get(i), dataType);
								//ps.setDouble(3, new Double(Double.parseDouble(vals.get(i).toString())));
								ps.addBatch();
							}
							ps.executeBatch();
						}
					}
				}
			}
		}
    }*/
    
     protected void setData(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	 //TODO parallelize this loop i.e. update for multiple time series fetches in parallel
		for (String tableName : parms.getWhereColumns().keySet()) { 
			if (parms.getSelectTables().get(tableName).equalsIgnoreCase(parms.getView().name())) {
				String id = null, cluster = null;
				int ln = -1, beginoffset = 0, endoffset = 0;
				long startts = -1;
				long endts = -1;
				//System.out.println(parms.getWhereColumns().get(tableName).keySet());
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {					
					if (colName.equalsIgnoreCase("clust")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("ts")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
						}
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">"))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">")).getTime() + 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">="))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">=")).getTime();
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<"))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<")).getTime() - 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<="))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<=")).getTime();		
					}
					if (colName.equalsIgnoreCase("off")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							beginoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
							endoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">"))
							beginoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get(">").toString()) + 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">="))
							beginoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get(">=").toString());
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<"))
							endoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("<").toString()) - 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<="))
							endoffset = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("<=").toString());		
					}
			}
				//System.out.println("cluster = " + cluster + "; ln = " + ln + "; id = " + id + "; startts = " + startts + "; endts = " + endts);				
				//Object[] blockQueryResults = dataHandler.readBlockData(cluster, ln, id, startts, beginoffset, endts, endoffset);
				//Frequency dataFrequency = (Frequency) blockQueryResults[1];
				//byte[] bulkData = (byte[]) blockQueryResults[2];
				byte[] bulkData = RESTFactory.decode(tsProxy.getBulkData(cluster, ln, id, startts, beginoffset, endts, endoffset).getBulkData());
				
				AvroTimeSeriesDeserializer atsd = new AvroTimeSeriesDeserializer();
				Schema.Type dataType = DynamicTypeTranslator.getSchemaType(DynamicTypeTranslator.getViewDataType(parms.getView()));
				atsd.resetSchema(dataType);
				atsd.setRawData(bulkData);
				//Schema.Type dataType = atsd.getDataType();
				int numOfBlocks = atsd.getBlockCount();
				PreparedStatement statement = connection.prepareStatement
		    			("INSERT INTO TIMESERIES." + parms.getView().name() + "(clust,ln,id,ts,off,val) VALUES (?,?,?,?,?,?);");
				boolean found = false;
				int count = 0;
				for (int i=0; i< numOfBlocks; i++) {
					int dataCount = atsd.getDataCount(i);
						if (!found)
							if (dataCount > 0)
								found = true;
						String cl1 = atsd.getCluster(i);
						String id1 = atsd.getGuid(i);
						int ln1 = atsd.getLn(i);
						long start = atsd.getStartts(i);
						MasterData md = mdHandler.getMasterData(cl1, ln1, id1, start);
						if (md == null)
							continue;
					for (int j=0; j< dataCount; j++) {
						statement.setString(1, cl1);
						statement.setInt(2, ln1);
		        		statement.setString(3, id1);
		        		if (TimeSeriesShard.ignoreOffsets(md.getFreq())) {
		        			long tsMillis = TimeSeriesShard.advance(md.getFreq(), start, (int) atsd.getOffset(i, j)).getMillis();
		        			statement.setTimestamp( 4, new Timestamp(tsMillis) );
		        			statement.setInt(5, 0); 
		        		}
		        		else {
		        			statement.setTimestamp( 4, new Timestamp(start) );
		        			statement.setInt(5, (int) atsd.getOffset(i, j)); 
		        		}
		        		DynamicTypeTranslator.setPreparedStatementValue(6, statement, atsd.getVal(i, j), dataType);
			       		statement.addBatch();
			       		count++;
			       		if (count >= queryLimit)
			       			break;
					}
					if (count >= queryLimit)
		       			break;
				}
				if (found)
					statement.executeBatch();
			}
		}
    }
    
/*    protected void setSampledData(ParsedParameters parms) throws TimeSeriesException, SQLException {
    	
		for (String tableName : parms.getWhereColumns().keySet()) { 
			if (tableName.equalsIgnoreCase(parms.getView().name())) {
				String id = null, cluster = null;
				int ln = -1;
				double sf = -1.0;
				long startts = -1;
				long endts = -1;
				for (String colName : parms.getWhereColumns().get(tableName).keySet()) {
					if (colName.equalsIgnoreCase("id")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							id = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("cluster")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							cluster = parms.getWhereColumns().get(tableName).get(colName).get("=").toString();
						}
					}
					if (colName.equalsIgnoreCase("ln")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							ln = Integer.parseInt(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("sf")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							sf = Double.parseDouble(parms.getWhereColumns().get(tableName).get(colName).get("=").toString());
						}
					}
					if (colName.equalsIgnoreCase("ts")) {
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("=")) {
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("=")).getTime();
						}
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">"))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">")).getTime() + 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains(">="))
							startts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get(">=")).getTime();
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<"))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<")).getTime() - 1;
						if (parms.getWhereColumns().get(tableName).get(colName).keySet().contains("<="))
							endts = ((java.sql.Timestamp) parms.getWhereColumns().get(tableName).get(colName).get("<=")).getTime();		
					}
				}
				long dataSize = (endts - startts + 1);
				ISampledDataHandler sdHandler = HandlerFactory.getSampledDataHandler();
				//byte[] bulkData = sdHandler.readBlockData(cluster, ln, id, startts, endts, (long) (dataSize*sf), dataSize);
				TimeSeries ts = sdHandler.readBlockData(cluster, ln, id, startts, 0, endts, 0, (int) (dataSize*sf));
				sdHandler.shutdown();
				//AvroTimeSeriesDeserializer atsd = new AvroTimeSeriesDeserializer();
				//Schema.Type dataType = DynamicTypeTranslator.getSchemaType(DynamicTypeTranslator.getViewDataType(parms.getView()));
				//atsd.resetSchema(dataType);
				//atsd.setRawData(bulkData);
				//Schema.Type dataType = atsd.getDataType();
				//int numOfBlocks = atsd.getBlockCount();
				PreparedStatement statement = connection.prepareStatement
		    			("INSERT INTO TIMESERIES." + parms.getView().name() + "(ln,id,ts,val,sf) VALUES (?,?,?,?,?);");
				boolean found = false;
				//for (int i=0; i< numOfBlocks; i++) {
					//int dataCount = atsd.getDataCount(i);
					//	if (!found)
					//		if (dataCount > 0)
					//			found = true;
					//	String id1 = atsd.getGuid(i);
					//	int ln1 = atsd.getLn(i);
					//	long start = atsd.getStartts(i);
					for (long j : ts.getData().keySet()) {	
						//statement.setInt(1, ln);
		        		//statement.setString(2, id);
			       		//statement.setTimestamp( 3, new Timestamp(start + j) );
			       		//DynamicTypeTranslator.setPreparedStatementValue(4, statement, atsd.getVal(i, j), dataType);
			       		//statement.setDouble(5, sf);
			       		//statement.addBatch();
					}
				//}
				//if (found)
					//statement.executeBatch();
			}
		}
    }*/
  
    /**
     * @see org.teiid.translator.jdbc.JDBCBaseExecution#close()
     */
    public synchronized void close() {
    	
        // first we would need to close the result set here then we can close
        // the statement, using the base class.
    	try {
	        if (results != null) {
	            try {
	            	if (!results.isClosed())
	            		results.close();
	                results = null;
	            } catch (SQLException e) {
	            	logger.debug("found sql exception while closing resultset:" + e.getMessage());
	            }
	        }
    	} finally {
    		super.close();
    	}
    }
}
