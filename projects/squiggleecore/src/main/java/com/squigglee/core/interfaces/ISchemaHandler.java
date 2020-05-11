// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.List;

import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesException;

public interface ISchemaHandler extends IHandler {
	public boolean createSchema(MasterData md) throws TimeSeriesException;
	public boolean createSchema(MasterData md, int offsetCount) throws TimeSeriesException;
	public void deleteSchema(String cluster, long id) throws TimeSeriesException;
	
	public boolean deletePatternIndexTables(List<Long> list, String index) throws TimeSeriesException;
	public boolean createPatternIndexTables(String cluster, long id, String idxTableName, String dataKeyspace) throws TimeSeriesException;
}
