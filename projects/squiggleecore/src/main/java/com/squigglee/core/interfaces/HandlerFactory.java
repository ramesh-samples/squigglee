// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.interfaces;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.IDataHandler;
import com.squigglee.core.interfaces.IIndexHandler;
import com.squigglee.core.interfaces.IMasterDataHandler;
import com.squigglee.core.interfaces.IPatternHandler;
import com.squigglee.core.interfaces.ISampledDataHandler;
import com.squigglee.core.interfaces.ISchemaHandler;
import com.squigglee.core.interfaces.ISketchHandler;
import com.squigglee.core.interfaces.IStatusHandler;

public final class HandlerFactory {
	private static Logger logger = Logger.getLogger("com.squigglee.core.interfaces.HandlerFactory");
	private static String _storageProvider = null;
	private static Map<String, IHandler> _cache = null;
	
	private HandlerFactory() {}
	
	public static void initialize(String storageProvider) {
		_storageProvider = storageProvider;
	}
	
	static {
		try {
			_cache = new HashMap<String, IHandler>();
			_storageProvider = LocalNodeProperties.getStorageProvider();
		} catch (TimeSeriesException e) {
			_storageProvider = "com.squigglee.storage.mbb";
		}
	}
	
	public static IStatusHandler getStatusHandler() throws TimeSeriesException {
		return (IStatusHandler) getInstance("IStatusHandler");
	}

	public static IDataHandler getDataHandler() throws TimeSeriesException {
		return (IDataHandler) getInstance("IDataHandler");
	}

	public static IPatternHandler getPatternHandler() throws TimeSeriesException {
		return (IPatternHandler) getInstance("IPatternHandler");
	}

	public static IIndexHandler getIndexHandler() throws TimeSeriesException {
		return (IIndexHandler) getInstance("IIndexHandler");
	}

	public static ISchemaHandler getSchemaHandler() throws TimeSeriesException {
		return (ISchemaHandler) getInstance("ISchemaHandler");
	}

	public static ISketchHandler getSketchHandler() throws TimeSeriesException {
		return (ISketchHandler) getInstance("ISketchHandler");
	}

	public static IMasterDataHandler getMasterDataHandler() throws TimeSeriesException {
		return (IMasterDataHandler) getInstance("IMasterDataHandler");
	}

	public static ISampledDataHandler getSampledDataHandler() throws TimeSeriesException {
		return (ISampledDataHandler) getInstance("ISampledDataHandler");
	}
	
	public static ICEPDataHandler getCEPDataHandler() throws TimeSeriesException {
		return (ICEPDataHandler) getInstance("ICEPDataHandler");
	}

	private static IHandler getInstance(String name) {
		try {
			if (!_cache.containsKey(name)) {
				IHandler instance = (IHandler) (Class.forName(_storageProvider + "." + name + "Impl")).newInstance();
				if (instance != null)
					instance.initialize();
				if (instance != null)
					_cache.put(name, instance);
			}
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | TimeSeriesException e) {
			logger.error("Found error instantiating class " + _storageProvider + "." + name, e);
		}
		return _cache.get(name);
	}
}
