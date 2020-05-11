// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.squigglee.core.config.TsrConstants;

public class TestUtility {
	
	public void setEnvProps(String clusterName, int clusterPort, String clusterSeeds, 
			int logicalNumber, int indexChunkSize, int indexNumChunks, int indexTimerInterval,
			String insertMode, String queryMode, String serializerType, int overlayTimerInterval, String vdbFile) {
		Map<String,String> newenv = new HashMap<String,String>();
		newenv.put(TsrConstants.CLUSTER_NAME, clusterName);
		newenv.put(TsrConstants.NODE_LOGICAL_NUMBER, logicalNumber + "");
		newenv.put(TsrConstants.INDEX_CHUNK_SIZE, indexChunkSize + "");
		newenv.put(TsrConstants.INDEX_NUM_CHUNKS, indexNumChunks + "");
		newenv.put(TsrConstants.HANDLER_SERIALIZER, serializerType);
		newenv.put(TsrConstants.OVERLAY_TIMER_INTERVAL, overlayTimerInterval + "");
		newenv.put(TsrConstants.OVERLAY_VDB_FILE, vdbFile);

		setEnv(newenv);
	}
	
	public void setEnvProps(String propFilePath) {
		Map<String,String> newenv = new HashMap<String,String>();
		newenv.put(TsrConstants.TSR_PROPERTIES_FILE, propFilePath);
		setEnv(newenv);
	}
	
	public void setPropertyFileLocation(String propFile) {
		Map<String,String> newenv = new HashMap<String,String>();
		newenv.put(TsrConstants.TSR_PROPERTIES_FILE, propFile);
		setEnv(newenv);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void setEnv(Map<String, String> newenv)
	{
	  try
	    {
	        Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
	        Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
	        theEnvironmentField.setAccessible(true);
	        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
	        env.putAll(newenv);
	        Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
	        theCaseInsensitiveEnvironmentField.setAccessible(true);
	        Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
	        cienv.putAll(newenv);
	    }
	    catch (NoSuchFieldException e)
	    {
	      try {
	        Class[] classes = Collections.class.getDeclaredClasses();
	        Map<String, String> env = System.getenv();
	        for(Class cl : classes) {
	            if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
	                Field field = cl.getDeclaredField("m");
	                field.setAccessible(true);
	                Object obj = field.get(env);
	                Map<String, String> map = (Map<String, String>) obj;
	                map.clear();
	                map.putAll(newenv);
	            }
	        }
	      } catch (Exception e2) {
	        e2.printStackTrace();
	      }
	    } catch (Exception e1) {
	        e1.printStackTrace();
	    } 
	}
}
