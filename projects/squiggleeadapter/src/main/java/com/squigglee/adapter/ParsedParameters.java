// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.adapter;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.CommandType;
import com.squigglee.core.entity.View;

public class ParsedParameters {
	
	private CommandType commandType = CommandType.SELECT;
	public CommandType getCommandType() { return this.commandType;}
	public void setCommandType(CommandType commandType) { this.commandType = commandType;}
	
	private List<Map<String, Object>> insertParameters = null;
	public List<Map<String, Object>> getInsertParameters() { return this.insertParameters; }
	public void setInsertParameters(List<Map<String, Object>> insertParameters) { this.insertParameters = insertParameters;}
	
	private List<Map<String, Object>> deleteParameters = null;
	public List<Map<String, Object>> getDeleteParameters() { return this.deleteParameters; }
	public void setDeleteParameters(List<Map<String, Object>> deleteParameters) { this.deleteParameters = deleteParameters;}
	
	//table name and associated columns
	private Map<String, String> selectTables = null;
	public Map<String, String> getSelectTables() { return this.selectTables; }
	public void setSelectTables(Map<String, String> selectTables) { this.selectTables = selectTables;}
	
	//table name and associated columns
	private Map<String, List<String>> selectColumns = null;
	public Map<String, List<String>> getSelectColumns() { return this.selectColumns; }
	public void setSelectColumns(Map<String, List<String>> selectColumns) { this.selectColumns = selectColumns;}
	
	//table name, column name, predicate, and value 
	private Map<String, Map<String,Map<String,Object>>> whereColumns = null;
	public Map<String, Map<String,Map<String,Object>>> getWhereColumns() { return this.whereColumns; }
	public void setWhereColumns(Map<String, Map<String,Map<String,Object>>> whereColumns) { this.whereColumns = whereColumns;}
	
	private View view = null;
	public View getView() { return this.view;}
	public void setView(View view) {this.view = view;}
	
}
