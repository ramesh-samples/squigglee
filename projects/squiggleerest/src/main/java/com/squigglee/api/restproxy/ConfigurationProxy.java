package com.squigglee.api.restproxy;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.IConfigRESTService;
import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.interfaces.IMasterDataHandler;

public class ConfigurationProxy {
	private static Logger logger = Logger.getLogger("com.squigglee.api.restproxy.TimeSeriesProxy");
	protected int localLn = 0;
	protected String localCluster = null;
	protected IMasterDataHandler mdHandler = null;
	
	public ConfigurationProxy(IMasterDataHandler mdHandler, int localLn, String localCluster) {
		this.mdHandler = mdHandler;
		this.localLn = localLn;
		this.localCluster = localCluster;
	}
	
	public TimeSeriesConfig createConfig(TimeSeriesConfig config) {
		try {
			List<Long> mdIds = new ArrayList<Long>();
			if (config.isDataLocal()) {
				mdIds = mdHandler.createMasterData(config);
			} else {
				TimeSeriesConfig dataLocalConfig = config.clone();
				dataLocalConfig.setDataLocal(true);
				List<Integer> replicas = mdHandler.getReplicaSet(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode());
				System.out.println("Replica set = " + replicas);
				for (int ln : replicas) {
					if (ln == dataLocalConfig.getLogicalNode()) {
						if (ln == localLn) {
							System.out.println("Data Local configuration for node " + ln);
							mdIds = mdHandler.createMasterData(dataLocalConfig);
						}
						else {
							System.out.println("Proxying create config request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
							IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
							proxyService.createConfigJSON(dataLocalConfig);
						}
						break;
					}
				}
				System.out.println("New master data ids created for config " + config + " are " + mdIds);
				for (int ln : replicas) {
					if (ln == dataLocalConfig.getLogicalNode())
						continue;
					if (ln == localLn) {
						System.out.println("Executing local create config request " + dataLocalConfig + " at node " + ln);
						mdIds = mdHandler.createMasterData(config);
					}
					else {
						System.out.println("Proxying create config request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
						IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
						proxyService.createConfigJSON(dataLocalConfig);
					}
				}
			}
			return config;
		} catch (Exception e) {
			logger.error("Found error creating configuration entries for config = " + config, e);
			config.setErrorMessage(e.getMessage());
		}
		return config;
	}
	
	public TimeSeriesConfig deleteConfig(TimeSeriesConfig config) {
		try {	
			List<Long> mdIds = new ArrayList<Long>();
			if (config.isDataLocal()) {
				mdIds = mdHandler.deleteMasterData(config);
			} else {
				TimeSeriesConfig dataLocalConfig = config.clone();
				dataLocalConfig.setDataLocal(true);
				List<Integer> replicas = mdHandler.getReplicaSet(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode());
				System.out.println("Replica set = " + replicas);
				
				for (int ln : replicas) {
					if (ln == config.getLogicalNode())
						continue;
					if (ln == localLn)
						mdIds = mdHandler.deleteMasterData(config);
					else {
						System.out.println("Proxying delete config request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
						IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
						proxyService.deleteConfigJSON(dataLocalConfig);
					}
				}
				//for deletes do the data node last
				for (int ln : replicas) {
					if (ln == dataLocalConfig.getLogicalNode()) {
						if (ln == localLn) {
							System.out.println("Data Local configuration for node " + ln);
							mdIds = mdHandler.deleteMasterData(dataLocalConfig);
						}
						else {
							System.out.println("Proxying delete config request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
							IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
							proxyService.deleteConfigJSON(dataLocalConfig);
						}
					}
				}
				System.out.println("Deleted master data ids are " + mdIds);
			}
			return config;
		} catch (Exception e) {
			logger.error("Found error creating configuration entries for config = " + config, e);
			config.setErrorMessage(e.getMessage());
		}
		return config;
	}
	
	public void addIndex(TimeSeriesConfig config) {
		try {
			if (config.isDataLocal()) {
				mdHandler.updateIndex(config.getCluster(), config.getLogicalNode(), config.getGuid(), config.getIndexes(), false);
			} else {
				TimeSeriesConfig dataLocalConfig = config.clone();
				dataLocalConfig.setDataLocal(true);
				List<Integer> replicas = mdHandler.getReplicaSet(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode());
				System.out.println("Replica set = " + replicas);
				for (int ln : replicas) {
					if (ln == dataLocalConfig.getLogicalNode()) {
						if (ln == localLn) {
							System.out.println("Data Local configuration for node " + ln);
							mdHandler.updateIndex(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode(), 
								dataLocalConfig.getGuid(), dataLocalConfig.getIndexes(), false);
						}
						else {
							System.out.println("Proxying update index request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
							IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
							proxyService.addIndexJSON(dataLocalConfig);
						}
					}
				}
				for (int ln : replicas) {
					if (ln == config.getLogicalNode())
						continue;
					if (ln == localLn)
						mdHandler.updateIndex(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode(), 
							dataLocalConfig.getGuid(), dataLocalConfig.getIndexes(), false);
					else {
						System.out.println("Proxying update index request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
						IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
						proxyService.addIndexJSON(dataLocalConfig);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Found error adding indexes " + config.getIndexes() + "for ln = " + config.getLogicalNode() 
					+ " id = " + config.getGuid() + " cluster = " + config.getCluster(), e);
		}
	}
	
	public void dropIndex(TimeSeriesConfig config) {
		try {
			if (config.isDataLocal()) {
				mdHandler.updateIndex(config.getCluster(), config.getLogicalNode(), config.getGuid(), config.getIndexes(), true);
			} else {
				TimeSeriesConfig dataLocalConfig = config.clone();
				dataLocalConfig.setDataLocal(true);
				List<Integer> replicas = mdHandler.getReplicaSet(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode());
				System.out.println("Replica set = " + replicas);
				
				for (int ln : replicas) {
					if (ln == config.getLogicalNode())
						continue;
					if (ln == localLn)
						mdHandler.updateIndex(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode(), 
							dataLocalConfig.getGuid(), dataLocalConfig.getIndexes(), true);
					else {
						System.out.println("Proxying update index request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
						IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
						proxyService.dropIndexJSON(dataLocalConfig);

					}
				}
				
				for (int ln : replicas) {
					if (ln == dataLocalConfig.getLogicalNode()) {
						if (ln == localLn) {
							System.out.println("Data Local configuration for node " + ln);
							mdHandler.updateIndex(dataLocalConfig.getCluster(), dataLocalConfig.getLogicalNode(), 
								dataLocalConfig.getGuid(), dataLocalConfig.getIndexes(), true);
						}
						else {
							System.out.println("Proxying update index request for node " + ln + " in cluster " + dataLocalConfig.getCluster());
							IConfigRESTService proxyService = RESTFactory.getConfigurationProxy(mdHandler.getLocation(dataLocalConfig.getCluster(), ln).getAddress());
							proxyService.dropIndexJSON(dataLocalConfig);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("Found error dropping indexes " + config.getIndexes() + "for ln = " + config.getLogicalNode() 
					+ " id = " + config.getGuid() + " cluster = " + config.getCluster(), e);
		}
	}
}
