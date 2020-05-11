// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.storage.cassandra.datastax;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.config.TsrConstants;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IHandlerFactory;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IOverlayHandler;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.interfaces.IStatusHandler;
import com.squigglee.core.interfaces.TimeSeriesException;

public class HandlerFactory implements IHandlerFactory {

	public HandlerFactory() throws TimeSeriesException {
		if (!LocalNodeProperties.getCassandraDriver().equals(TsrConstants.DATASTAX))
			throw new TimeSeriesException("To use this factory method the driver type must be set to the Datastax Driver in properties file");
	}
	
	@Override
	public IStatusHandler getNewStatusHandler() throws TimeSeriesException {
		IStatusHandler handler = new com.squigglee.core.storage.cassandra.datastax.StatusHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public IDataHandler getNewDataHandler() throws TimeSeriesException {
		IDataHandler handler = new com.squigglee.core.storage.cassandra.datastax.DataHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public IPatternHandler getNewPatternHandler() throws TimeSeriesException {
		IPatternHandler handler = new com.squigglee.core.storage.cassandra.datastax.PatternHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public IIndexHandler getNewIndexHandler() throws TimeSeriesException {
		IIndexHandler handler = new com.squigglee.core.storage.cassandra.datastax.IndexHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public ISchemaHandler getNewSchemaHandler() throws TimeSeriesException {
		ISchemaHandler handler = new com.squigglee.core.storage.cassandra.datastax.SchemaHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public ISketchHandler getNewSketchHandler() throws TimeSeriesException {
		ISketchHandler handler = new com.squigglee.core.storage.cassandra.datastax.SketchHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public IOverlayHandler getNewOverlayandler() throws TimeSeriesException {
		IOverlayHandler handler = new com.squigglee.core.storage.cassandra.datastax.OverlayHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public IMasterDataHandler getNewMasterDataHandler() throws TimeSeriesException {
		IMasterDataHandler handler = new com.squigglee.core.storage.cassandra.datastax.MasterDataHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

	@Override
	public ISampledDataHandler getNewSampledDataHandler() throws TimeSeriesException {
		ISampledDataHandler handler = new com.squigglee.core.storage.cassandra.datastax.SampledDataHandlerImpl(
				LocalNodeProperties.getClusterName(), LocalNodeProperties.getClusterPort(), LocalNodeProperties.getClusterSeeds(),
				LocalNodeProperties.getSerializerType(), LocalNodeProperties.getLocalDataCenter(), LocalNodeProperties.getNodeAddress());
		handler.initialize();
		return handler;
	}

}
