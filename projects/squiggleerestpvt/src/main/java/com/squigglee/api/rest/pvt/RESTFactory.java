package com.squigglee.api.rest.pvt;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jboss.resteasy.client.ProxyFactory;
import org.jboss.resteasy.util.Base64;

public class RESTFactory {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.pvt.RESTFactory");
	protected String path = "/squiggleerestui";
	protected int port = 8080;
	protected String transport = "http";
	protected String address = "127.0.0.1";
	protected int connectionTimeoutMillis = 20000;
	protected int socketTimeoutMillis = 20000;
	
	public RESTFactory(String path, int port, String transport, int connectionTimeoutMillis, int socketTimeoutMillis) {
		this.path = path;
		this.port = port;
		this.transport = transport;
		this.connectionTimeoutMillis = connectionTimeoutMillis;
		this.socketTimeoutMillis = socketTimeoutMillis;
	}
	
	public RESTFactory() {}
	
	public Object getProxy(String addr, Class<?> interfaceClass) {
		String fullpath = transport + "://" + addr + ":" + port + path;
		System.out.println("Creating time series data proxy for path = " + fullpath);
		
		//DefaultHttpClient httpClient = new DefaultHttpClient();
		//HttpParams params = httpClient.getParams();
		//HttpConnectionParams.setConnectionTimeout(params, connectionTimeoutMillis);
		//HttpConnectionParams.setSoTimeout(params, socketTimeoutMillis);
		//ClientExecutor executor = new ApacheHttpClient4Executor(httpClient);
		//return ProxyFactory.create(interfaceClass, fullpath, executor);
		return ProxyFactory.create(interfaceClass, fullpath);
	}
	
	public byte[] decode(String bulkDataString) {
		try {
			return Base64.decode(bulkDataString);
		} catch (IOException e) {
			logger.error("Error decoding bulk data string " + bulkDataString, e);
		}
		return null;
	}
	
	public String encode(byte[] bulkDataBytes) {
		return Base64.encodeBytes(bulkDataBytes);
	}
}