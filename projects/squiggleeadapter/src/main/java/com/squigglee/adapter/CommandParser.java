// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.AndOr;
import org.teiid.language.BatchedCommand;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.Update;

import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.entity.View;

public class CommandParser {
	private Command command = null;
	private ParsedParameters parms = null;
	public CommandParser(Command command) throws TimeSeriesException {
		this.command = command;
		parseCommand();
	}
	
	public Command getCommand() { return this.command;}
	public ParsedParameters getParsedParameters() { return this.parms;}
	
	public void parseCommand() throws TimeSeriesException {
		parms = new ParsedParameters();
    	parms.setView(getView());
    	parms.setCommandType(getCommandType());
		if (parms.getCommandType().equals(CommandType.SELECT) ) {
			parseSelectFrom((Select) command);
			parseSelectColumns((Select) command);
			parseSelectWhere((Select) command);
		}
		if (parms.getCommandType().equals(CommandType.INSERT) )
			parseInsertCommand();
		else if (parms.getCommandType().equals(CommandType.DELETE) )
			parseDeleteCommand();
	}
	
	protected void parseInsertCommand() throws TimeSeriesException {
		List<Map<String, Object>> insertParameters = new ArrayList<Map<String,Object>>();
		Insert insert = ((Insert) command);
		List<ColumnReference> cols = insert.getColumns();
		if (command instanceof BatchedCommand && ((BatchedCommand) command).getParameterValues() != null) {
			BatchedCommand bc = (BatchedCommand) command;
			Iterator<?> it = bc.getParameterValues();
			while (it.hasNext()) {
				Map<String, Object> map = new HashMap<String, Object>();
				@SuppressWarnings("unchecked")
				List<Object> list = (List<Object>) it.next();
				for (int i = 0; i < list.size(); i++) {
					map.put(cols.get(i).getName().toLowerCase(), list.get(i));
				}
				insertParameters.add(map);
			}
		} else {
			Map<String, Object> map = new HashMap<String, Object>();
			ExpressionValueSource evs = (ExpressionValueSource) insert.getValueSource();
			List<Expression> values = evs.getValues();
			for (int i = 0; i < cols.size(); i++) {
				map.put(cols.get(i).getName().toLowerCase(), ((Literal) values.get(i)).getValue());
			}
			insertParameters.add(map);
		}
		parms.setInsertParameters(insertParameters);
	}
	
	protected void parseDeleteCommand() throws TimeSeriesException {
		List<Map<String, Object>> deleteParameters = new ArrayList<Map<String,Object>>();
		Delete delete = ((Delete) command);
		Map<String, Object> map = new HashMap<String,Object>();
		if (delete.getWhere() instanceof Comparison) {
			parseComparison( (Comparison) delete.getWhere() );
		} else if (delete.getWhere() instanceof AndOr) {
			AndOr andOr = (AndOr) delete.getWhere();
	    	while (andOr != null) {
		    	Condition left = andOr.getLeftCondition();
		    	//Operator operator = andOr.getOperator();
		    	Condition right = andOr.getRightCondition();
		    	if (left instanceof Comparison) {
		    		Comparison comp = (Comparison) left;
		    		String[] parsedCol = parseWhereColumnName(((ColumnReference) comp.getLeftExpression()).toString()); //g_0."ID"
		    		//String op = comp.getOperator().toString();
		    		Object val = ((Literal) comp.getRightExpression()).getValue();
		    		map.put(stripQuotes(parsedCol[parsedCol.length-1].toLowerCase()), val);
		    	}
		    	if (right instanceof AndOr)
		    		andOr = (AndOr) right;
		    	else if (right instanceof Comparison) {
		    		Comparison comp = (Comparison) right;
		    		String[] parsedCol = parseWhereColumnName(((ColumnReference) comp.getLeftExpression()).toString()); //g_0."ID"
		    		//String op = comp.getOperator().toString();
		    		Object val = ((Literal) comp.getRightExpression()).getValue();
		    		map.put(stripQuotes(parsedCol[parsedCol.length-1].toLowerCase()), val);
		    		break;
		    	}
	    	}
		}
		deleteParameters.add(map);
		parms.setDeleteParameters(deleteParameters);
	}
	
	protected void parseSelectFrom(Select select) {
		List<TableReference> tables = select.getFrom();
		if (parms.getSelectTables() == null)
			parms.setSelectTables(new HashMap<String,String>());
		if (parms.getSelectColumns() == null)
			parms.setSelectColumns(new HashMap<String,List<String>>());
		
		for (TableReference tr : tables) {
			String[] parsed = parseTableName(tr.toString());
			//if (!parms.getSelectColumns().containsKey(parsed[0]))
			//	parms.getSelectColumns().put(parsed[0], new ArrayList<String>());
			//parms.getSelectTables().put(parsed[0], parsed[1]);
			if (!parms.getSelectColumns().containsKey(parsed[1]))
				parms.getSelectColumns().put(parsed[1], new ArrayList<String>());
			parms.getSelectTables().put(parsed[1], parsed[0]);
		}
	}
	
	protected void parseSelectColumns(Select select) {
		List<DerivedColumn> derivedColumns = select.getDerivedColumns();
		for (DerivedColumn dc : derivedColumns) {	//g_0."VAL"
			for (TableReference tr : select.getFrom()) {	//"CASSANDRA"."TIMESERIES"."DOUBLEPATTERNS" AS g_0
				String[] parsed = parseTableName(tr.toString());
				String[] parsedCol = parseColumnName(dc.toString());
				if (parsed[1].equalsIgnoreCase(parsedCol[1]))
					if ( !parms.getSelectColumns().get(parsed[1]).contains( parsedCol[0] ) )
						parms.getSelectColumns().get(parsed[1]).add(parsedCol[0]);
					//if ( !parms.getSelectColumns().get(parsed[0]).contains( parsedCol[1] ) )
					//	parms.getSelectColumns().get(parsed[0]).add(parsedCol[0]);
			}
		}
	}
	
	//table name, column name, predicate, and value 
	protected void parseSelectWhere(Select select) {
		if (parms.getWhereColumns() == null)
			parms.setWhereColumns(new HashMap<String, Map<String,Map<String,Object>>>());
		
		if (select.getWhere() instanceof Comparison) {
				parseComparison( (Comparison) select.getWhere() );
		} else if (select.getWhere() instanceof AndOr) {
			AndOr andOr = (AndOr) select.getWhere();
	    	while (andOr != null) {
		    	Condition left = andOr.getLeftCondition();
		    	//Operator operator = andOr.getOperator();
		    	Condition right = andOr.getRightCondition();
		    	if (left instanceof Comparison) {
		    		parseComparison( (Comparison) left);
		    	}
		    	if (right instanceof AndOr)
		    		andOr = (AndOr) right;
		    	else if (right instanceof Comparison) {
		    		parseComparison( (Comparison) right);
		    		break;
		    	}
	    	}
		}
	}
			
	protected CommandType getCommandType() {
    	if (command instanceof Insert) 
    		return CommandType.INSERT;
    	if (command instanceof Update) 
    		return CommandType.UPDATE;
    	if (command instanceof Delete) 
    		return CommandType.DELETE;
    	if (command instanceof Select) 
    		return CommandType.SELECT;
    	
    	return CommandType.SELECT;
   	}

	protected View getView() throws TimeSeriesException {
		String dataTableName = null;
    	if (command instanceof Insert) {
    		Insert insert = (Insert) command;
    		dataTableName = insert.getTable().getName().toUpperCase();
    	}
    	if (command instanceof Update) {
    		Update update = (Update) command;
    		dataTableName = update.getTable().getName().toUpperCase();
    	}
    	if (command instanceof Delete) {
    		Delete delete = (Delete) command;
    		dataTableName = delete.getTable().getName().toUpperCase();
    	}
    	if (command instanceof Select) {
    		Select select = (Select) command;
    		for (TableReference tableref : select.getFrom()) {
    			if (tableref instanceof NamedTable) {
    				if (dataTableName != null && !dataTableName.equalsIgnoreCase(((NamedTable)tableref).getName().toUpperCase()))
    					throw new TimeSeriesException();
    				else
    					dataTableName = ((NamedTable)tableref).getName().toUpperCase();
    			}
    		}
    	}
    	return Enum.valueOf(View.class, dataTableName.substring(dataTableName.lastIndexOf(".")+1).toUpperCase());
	}

	protected void parseComparison(Comparison comp) {
		String[] parsedCol = parseColumnName(((ColumnReference) comp.getLeftExpression()).toString()); //g_0."PGUID"
		if (!(comp.getRightExpression() instanceof Literal))
			return;
		String op = comp.getOperator().toString();
		Object val = ((Literal) comp.getRightExpression()).getValue();
		for (String tableName : parms.getSelectTables().keySet()) {
			//if (parsedCol[1].equalsIgnoreCase(parms.getSelectTables().get(tableName))) {
			if (parsedCol[1].equalsIgnoreCase(tableName)) {
				if (!parms.getWhereColumns().containsKey(tableName))
					parms.getWhereColumns().put(tableName, new HashMap<String,Map<String,Object>>());
				if (!parms.getWhereColumns().get(tableName).containsKey(parsedCol[0]))
					parms.getWhereColumns().get(tableName).put(parsedCol[0], new HashMap<String,Object>());
				parms.getWhereColumns().get(tableName).get(parsedCol[0]).put(op, val);
			}
		}

	}
	
	protected String stripQuotes(String val) {
		if (val.startsWith("'"))
			val = val.substring(1);
		if (val.endsWith("'"))
			val = val.substring(0,val.length()-1);

		if (val.startsWith("\""))
			val = val.substring(1);
		if (val.endsWith("\""))
			val = val.substring(0,val.length()-1);

		return val;
	}
	
	//table name and table alias in that order 
	protected String[] parseTableName(String fromClause) {
		String[] tokens = fromClause.split(" ");
		String qualifiedName = tokens[0];
		String alias = tokens[tokens.length-1];
		tokens = qualifiedName.split("\\.");
		return new String[]{stripQuotes(tokens[tokens.length-1]),stripQuotes(alias)};
	}
	
	//column name and table alias in that order 
	protected String[] parseWhereColumnName(String colRef) {
		String [] tokens = colRef.split("\\.");
		return tokens;
	}	
	
	//column name and table alias in that order 
	protected String[] parseColumnName(String colRef) {
		String [] tokens = colRef.split("\\.");
		return new String[]{stripQuotes(tokens[tokens.length-1]),stripQuotes(tokens[0])};
	}
	
}
