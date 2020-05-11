package com.squigglee.coord.utility;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.squigglee.coord.interfaces.ICEPService;
import com.squigglee.coord.interfaces.IConfigService;
import com.squigglee.coord.interfaces.ICoordService;
import com.squigglee.coord.interfaces.IDataService;
import com.squigglee.coord.interfaces.IEntitlementService;
import com.squigglee.coord.interfaces.IIndexService;
import com.squigglee.coord.interfaces.IPatternService;
import com.squigglee.coord.interfaces.IStatusService;
import com.squigglee.coord.interfaces.ITaskService;
import com.squigglee.core.entity.TimeSeriesException;

public final class ServiceFactory {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.utility.ServiceFactory");
	private static ZooKeeper zk = null;
	private static ZooKeeper zkov = null;
	private static Map<String, ICoordService> _cache = null;
	
	static {
		try {
			_cache = new HashMap<String,ICoordService>();
			zk = ZooKeeperFactory.getLocalZooKeeper();
			zkov = ZooKeeperFactory.getLocalOverlayZooKeeper();
		} catch (TimeSeriesException e) {
			logger.error("Error initializing local zookeeper",e);
		}
	}
	
	public static IStatusService getStatusService() throws TimeSeriesException {
		return (IStatusService) getServiceInstance("IStatusService");
	}
	
	public static IDataService getDataService() throws TimeSeriesException {
		return (IDataService) getServiceInstance("IDataService");
	}
	
	public static IConfigService getConfigurationService() throws TimeSeriesException {
		return (IConfigService) getServiceInstance("IConfigService");
	}
	
	public static ITaskService getTaskService() throws TimeSeriesException {
		return (ITaskService) getServiceInstance("ITaskService");
	}
	
	public static ICoordService getCoordinationService() throws TimeSeriesException {
		return getServiceInstance("ICoordService");
	}
	
	public static IEntitlementService getEntitlementService() throws TimeSeriesException {
		return (IEntitlementService) getServiceInstance("IEntitlementService");
	}

	public static IIndexService getIndexService() throws TimeSeriesException {
		return (IIndexService) getServiceInstance("IIndexService");
	}

	public static IPatternService getPatternService() throws TimeSeriesException {
		return (IPatternService) getServiceInstance("IPatternService");
	}
	
	//public static ISyncService getSyncService() throws TimeSeriesException {
	//	return (ISyncService) getServiceInstance("ISyncService");
	//}
	
	public static ICEPService getCEPService() throws TimeSeriesException {
		return (ICEPService) getServiceInstance("ICEPService");
	}
	
	private static ICoordService getServiceInstance(String name) {
		try {
			if (!_cache.containsKey(name)) {
				ICoordService instance = (ICoordService) (Class.forName("com.squigglee.coord.zk." + name + "Impl")).newInstance();
				if (instance != null)
					instance.initialize(zk, zkov);
				if (instance != null)
					_cache.put(name, instance);
			}
		} catch (InstantiationException e) {
			logger.error("Found error instantiating class " + "com.squigglee.coord.zk." + name, e);
		} catch (IllegalAccessException e) {
			logger.error("Found error instantiating class " + "com.squigglee.coord.zk." + name, e);
		} catch (ClassNotFoundException e) {
			logger.error("Found error instantiating class " + "com.squigglee.coord.zk." + name, e);
		} catch (TimeSeriesException e) {
			logger.error("Found error instantiating class " + "com.squigglee.coord.zk." + name, e);
		}
		return _cache.get(name);
	}
}
