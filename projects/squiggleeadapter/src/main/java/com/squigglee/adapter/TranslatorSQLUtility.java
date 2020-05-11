// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.Update;
import org.teiid.metadata.Column;

import com.squigglee.core.entity.View;

public class TranslatorSQLUtility {
	
	public static boolean verifySourceTable(Command command, String tableName) {
    	if (command instanceof Insert) {
    		Insert insert = (Insert) command;
    		if (insert.getTable().getName().toUpperCase().contains(tableName.toUpperCase()))
    			return true;
    	}
    	if (command instanceof Update) {
    		Update update = (Update) command;
    		if (update.getTable().getName().toUpperCase().contains(tableName.toUpperCase()))
    			return true;
    	}
    	if (command instanceof Select) {
    		Select select = (Select) command;
    		for (TableReference tableref : select.getFrom()) {
    			if (tableref instanceof NamedTable && ((NamedTable)tableref).getName().toUpperCase().contains(tableName.toUpperCase()))
    				return true;
    		}
    	}
    	return false;
	}
	  
    public static Object[] parseBulkQueryParameters(Command command) {
		Object guid = null;
		Object startts = null;
		Object endts = null;
		
    	Select select = (Select) command;
    	AndOr andOr = (AndOr) select.getWhere();
    	while (andOr != null) {
	    	Condition left = andOr.getLeftCondition();
	    	//Operator operator = andOr.getOperator();
	    	Condition right = andOr.getRightCondition();
	    	if (left instanceof Comparison) {
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("ID"))
	    			guid = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("STARTTS"))
	    			startts = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("ENDTS"))
	    			endts = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    	}
	    	if (right instanceof AndOr)
	    		andOr = (AndOr) right;
	    	else if (right instanceof Comparison) {
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("ID"))
	    			guid = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("STARTTS"))
	    			startts = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("ENDTS"))
	    			endts = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    		break;
	    	}
    	}
    	return new Object[]{guid, startts, endts};
    }

    public static Object[] parseMasterDataQueryParameters(Command command) {
		Object guid = null;
		
    	Select select = (Select) command;
    	Comparison comp = (Comparison) select.getWhere();
    	Expression left = comp.getLeftExpression();
    	Expression right = comp.getRightExpression();
    	if (left.toString().toUpperCase().contains("ID"))
    		guid = ((Literal) right).getValue();
    	return new Object[]{guid};
    }
	
    public static View getAPIDataType(TableReference table) {
		String[] parsed = TranslatorSQLUtility.parseTableName(table.toString());
		return View.valueOf(parsed[0]);
    }

    public static Object[] parseQueryParameters(Command command) {
		Object guid = null;
		Object startts = null;
		Object endts = null;
		Object sketchFactor = null;
    	Select select = (Select) command;
    	AndOr andOr = (AndOr) select.getWhere();
    	while (andOr != null) {
	    	Condition left = andOr.getLeftCondition();
	    	//Operator operator = andOr.getOperator();
	    	Condition right = andOr.getRightCondition();
	    	if (left instanceof Comparison) {
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("ID"))
	    			guid = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("SKETCHFACTOR"))
	    			sketchFactor = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    		if (((Comparison) left).getLeftExpression().toString().toUpperCase().contains("TS")) {
	    			if (((Comparison) left).getOperator().equals(Comparison.Operator.LE))
	    				endts = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    			else
		    			if (((Comparison) left).getOperator().equals(Comparison.Operator.GE))
		    				startts = ((Literal) ((Comparison) left).getRightExpression()).getValue();
	    		}
	    	}
	    	if (right instanceof AndOr)
	    		andOr = (AndOr) right;
	    	else if (right instanceof Comparison) {
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("ID"))
	    			guid = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("SKETCHFACTOR"))
	    			sketchFactor = ((Literal) ((Comparison) right).getRightExpression()).getValue();	    		
	    		if (((Comparison) right).getLeftExpression().toString().toUpperCase().contains("TS")) {
	    			if (((Comparison) right).getOperator().equals(Comparison.Operator.LE))
	    				endts = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    			else
		    			if (((Comparison) right).getOperator().equals(Comparison.Operator.GE))
		    				startts = ((Literal) ((Comparison) right).getRightExpression()).getValue();
	    		}
	    		break;
	    	}
    	}
    	return new Object[]{guid, startts, endts, sketchFactor};
    }

    public static List<Comparison> parseTimeRangeCriteria(Command command, String alias) {
    	List<Comparison> list = new ArrayList<Comparison>();
    	Select select = (Select) command;
    	AndOr andOr = (AndOr) select.getWhere();
    	while (andOr != null) {
	    	Condition left = andOr.getLeftCondition();
	    	//Operator operator = andOr.getOperator();
	    	Condition right = andOr.getRightCondition();
	    	Comparison comp = null;
	    	if (left instanceof Comparison) {
	    		comp = (Comparison) left;
	    		if (comp.getLeftExpression().toString().toUpperCase().contains("TS")) {
	    			ColumnReference col = (ColumnReference) comp.getLeftExpression();
	    			if (col.getTable().getCorrelationName().equalsIgnoreCase(alias))
	    				list.add(comp);
	    		}
	    	}
	    	if (right instanceof AndOr)
	    		andOr = (AndOr) right;
	    	else if (right instanceof Comparison) {
	    		comp = (Comparison) right;
	    		if (comp.getLeftExpression().toString().toUpperCase().contains("TS")) {
	    			ColumnReference col = (ColumnReference) comp.getLeftExpression();
	    			if (col.getTable().getCorrelationName().equalsIgnoreCase(alias))
	    				list.add(comp);
	    		}
	    		break;
	    	}
    	}
    	return list;
    }

    public static List<DerivedColumn> parseProjectedColumns(Command command, String alias) {
    	List<DerivedColumn> list = new ArrayList<DerivedColumn>();
    	Select select = (Select) command;
		List<DerivedColumn> derivedColumns = select.getDerivedColumns();
		
    	for (DerivedColumn col : derivedColumns) {
    		if (col.getAlias().equalsIgnoreCase(alias))
    			list.add(col);
    	}  	
    	return list;
    }
    
    public static Map<String,String> parseWhere(Command command, Map<String,String> guidMap) {
    	Map<String,String> map = new HashMap<String,String>();
    	
    	
    	return map;
    }
       
	public static String[] parseTableName(String fromClause) {
		String[] tokens = fromClause.split(" ");
		String qualifiedName = tokens[0];
		String alias = tokens[tokens.length-1];
		tokens = qualifiedName.split("\\.");
		return new String[]{tokens[0], alias, tokens[tokens.length-1] };
	}
    
	public static DerivedColumn getDerivedColumn(NamedTable nt, String colName, Class<?> type) {
		Column column = new Column();
		column.setName(colName);
		ColumnReference columnReference = new ColumnReference(nt, colName, column, type);
		return new DerivedColumn(colName,columnReference);
	}

}
