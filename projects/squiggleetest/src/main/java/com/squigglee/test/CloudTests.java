// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.test;

//import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.squigglee.cloud.ec2.EC2AnsibleCloudHandler;
import com.squigglee.cloud.interfaces.ICloudHandler;
import com.squigglee.core.entity.TimeSeriesException;
 
public class CloudTests {
	
	//@org.junit.Test
	public void launchSingleNode() throws Exception {
		(new TestUtility()).setPropertyFileLocation("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//String propFile = System.getenv(TranslatorConstants.TSR_PROPERTIES_FILE);
		
		ICloudHandler handler = new EC2AnsibleCloudHandler();
		Map<String,String> newNode = new HashMap<String,String>();
		newNode.put("ln", "1");
		newNode.put("dc", "us-east-1");
		newNode.put("stype", "Medium");
		newNode.put("storage", "100");
		List<Map<String,String>> topology = new ArrayList<Map<String,String>>();
		topology.add(newNode);
		int result = handler.createTopology(topology);
		System.out.println("Result of create topology request = " + result);
		if (result != 0)
			throw new TimeSeriesException("Failed to create new node");
		
		System.in.read();
	}

	//@org.junit.Test
	public void configureAllNodes() throws TimeSeriesException {
		(new TestUtility()).setPropertyFileLocation("/Users/AgnitioWorks/Documents/tsr/ansible/LocalNodeProperties.config");
		//String propFile = System.getenv(TranslatorConstants.TSR_PROPERTIES_FILE);
		ICloudHandler handler = new EC2AnsibleCloudHandler();
		int result = handler.configureNode("TestCluster", 0, 0);
		System.out.println("Result of configuring node 0 = " + result);
		if (result != 0)
			throw new TimeSeriesException("Failed to configure new node");
	}
}