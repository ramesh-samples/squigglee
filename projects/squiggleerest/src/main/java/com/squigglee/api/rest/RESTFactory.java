package com.squigglee.api.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;
import org.jboss.resteasy.client.ClientExecutor;
import org.jboss.resteasy.client.ProxyFactory;
import org.jboss.resteasy.client.core.executors.ApacheHttpClient4Executor;
import org.jboss.resteasy.util.Base64;

public final class RESTFactory {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.RESTFactory");
	private static String _path = "/squiggleerestui";
	private static int _port = 8080;
	private static String _transport = "http";
	//private static String _address = "127.0.0.1";
	private static int _connectionTimeoutMillis = 20000;
	private static int _socketTimeoutMillis = 20000;
	private static Map<Class<?>,Map<String, Object>> proxyCache = null;
	
	private RESTFactory() {	}
	
	static {
		proxyCache = new HashMap<Class<?>,Map<String, Object>>();
	}
	
	public static void initialize(String path, int port, String transport, int connectionTimeoutMillis, int socketTimeoutMillis) {
		_path = path;
		_port = port;
		_transport = transport;
		_connectionTimeoutMillis = connectionTimeoutMillis;
		_socketTimeoutMillis = socketTimeoutMillis;
	}
	
	@SuppressWarnings("deprecation")
	private static Object getProxy(String addr, Class<?> interfaceClass) {
		
		if (!proxyCache.containsKey(interfaceClass))
			proxyCache.put(interfaceClass, new HashMap<String,Object>());
		
		if (!proxyCache.get(interfaceClass).containsKey(addr)) {
			String fullpath = _transport + "://" + addr + ":" + _port + _path;
			//Object obj = ProxyFactory.create(interfaceClass, fullpath);
			
			HttpClient httpClient = new DefaultHttpClient();
			ClientConnectionManager mgr = httpClient.getConnectionManager();
			HttpParams params = httpClient.getParams();
			HttpConnectionParams.setConnectionTimeout(params, _connectionTimeoutMillis);
			HttpConnectionParams.setSoTimeout(params, _socketTimeoutMillis);
			httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(params, 
		            mgr.getSchemeRegistry()), params);
			ClientExecutor executor = new ApacheHttpClient4Executor(httpClient);
			Object obj = ProxyFactory.create(interfaceClass, fullpath, executor);
			
			if (obj != null)
				proxyCache.get(interfaceClass).put(addr, obj);
			System.out.println("Created and cached proxy for interface class " + interfaceClass + " and _path = " + fullpath);
			logger.debug("Created and cached proxy for interface class " + interfaceClass + " and _path = " + fullpath);
		}
		return proxyCache.get(interfaceClass).get(addr);
	}
	
	public static ITimeSeriesRESTService getTimeSeriesProxy(String addr) {
		return (ITimeSeriesRESTService) getProxy(addr, ITimeSeriesRESTService.class);
	}
	
	public static IPatternRESTService getPatternProxy(String addr) {
		return (IPatternRESTService) getProxy(addr, IPatternRESTService.class);
	}
	
	public static ISynopsesRESTService getSynopsesProxy(String addr) {
		return (ISynopsesRESTService) getProxy(addr, ISynopsesRESTService.class);
	}
	
	public static IConfigRESTService getConfigurationProxy(String addr) {
		return (IConfigRESTService) getProxy(addr, IConfigRESTService.class);
	}
	
	public static IStatusRESTService getStatusProxy(String addr) {
		return (IStatusRESTService) getProxy(addr, IStatusRESTService.class);
	}
	
	public static IOperatorRESTService getOperatorProxy(String addr) {
		return (IOperatorRESTService) getProxy(addr, IOperatorRESTService.class);
	}
	
	public static IIndexSchemaRESTService getIndexSchemaProxy(String addr) {
		return (IIndexSchemaRESTService) getProxy(addr, IIndexSchemaRESTService.class);
	}
	
	public static IEventRESTService getEventProxy(String addr) {
		return (IEventRESTService) getProxy(addr, IEventRESTService.class);
	}
	
	public static byte[] decode(String bulkDataString) {
		try {
			return Base64.decode(bulkDataString);
		} catch (IOException e) {
			logger.error("Error decoding bulk data string " + bulkDataString, e);
		}
		return null;
	}
	
	public static String encode(byte[] bulkDataBytes) {
		return Base64.encodeBytes(bulkDataBytes);
	}
}