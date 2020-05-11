package com.squigglee.cloud.ec2;

import java.util.Arrays;
import java.util.List;

//TODO refactor later to get lookups from EC2 client SDK/API 
public class EC2Mapper {

	public List<String> getRegions() {
		return Arrays.asList(new String[]{"ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", 
				"eu-west-1", "sa-east-1", "us-east-1", "us-west-1", "us-west-2"});
	}

	public List<String> getTypes() {
		return Arrays.asList(new String[]{"m3.large", "m3.large", "i2.xlarge", "i2.2xlarge"});
	}
	
	//TODO generalize via EC2 api calls
	public String getType(String type) {
		if (type == null)
			return "i2.xlarge";
		if (type.equalsIgnoreCase("Medium"))
			return "i2.xlarge";
		if (type.equalsIgnoreCase("Micro"))
			return "m3.large";
		if (type.equalsIgnoreCase("Large"))
			return "i2.xlarge";
		if (type.equalsIgnoreCase("Small"))
			return "m3.large";
		
		return "c4.2xlarge";
	}

	public String getReplicationDataCenter(String region) {
		if (region.equalsIgnoreCase("us-east-1"))
			return "us-east";
		if (region.equalsIgnoreCase("us-west-1"))
			return "us-west";
		if (region.equalsIgnoreCase("us-west-2"))
			return "us-west-2";
		if (region.equalsIgnoreCase("eu-west-1"))
			return "eu-west";
		if (region.equalsIgnoreCase("eu-central-1"))
			return "eu-central";
		if (region.equalsIgnoreCase("ap-northeast-1"))
			return "ap-northeast";
		if (region.equalsIgnoreCase("ap-southeast-1"))
			return "ap-southeast";
		if (region.equalsIgnoreCase("ap-southeast-2"))
			return "ap-southeast-2";
		if (region.equalsIgnoreCase("sa-east-1"))
			return "sa-east";
		return "us-east";
	}
	
	//TODO generalize via EC2 api calls 
	public String getImage(String region) {
		if (region.equalsIgnoreCase("us-east-1"))
			return "ami-76817c1e";
		if (region.equalsIgnoreCase("us-west-1"))
			return "ami-f0d3d4b5";
		if (region.equalsIgnoreCase("us-west-2"))
			return "ami-d13845e1";
		if (region.equalsIgnoreCase("eu-west-1"))
			return "ami-6e7bd919";
		if (region.equalsIgnoreCase("eu-central-1"))
			return "ami-b43503a9";
		if (region.equalsIgnoreCase("ap-northeast-1"))
			return "ami-4985b048";
		if (region.equalsIgnoreCase("ap-southeast-1"))
			return "ami-ac5c7afe";
		if (region.equalsIgnoreCase("ap-southeast-2"))
			return "ami-63f79559";
		if (region.equalsIgnoreCase("sa-east-1"))
			return "ami-b52890a8";
		
		return null;
	}
	
	//TODO generalize via EC2 api calls 
	public String getZone(String region) {
		if (region.equalsIgnoreCase("us-east-1"))
			return "us-east-1a";	//Virginia
		if (region.equalsIgnoreCase("us-west-1"))
			return "us-west-1b";	//N. California
		if (region.equalsIgnoreCase("us-west-2"))
			return "us-west-2a";	//Oregon
		if (region.equalsIgnoreCase("eu-west-1"))
			return "eu-west-1a";	//Ireland
		if (region.equalsIgnoreCase("eu-central-1"))
			return "eu-central-1a";	//Frankfurt
		if (region.equalsIgnoreCase("ap-northeast-1"))
			return "ap-northeast-1b";	//Tokyo
		if (region.equalsIgnoreCase("ap-southeast-1"))
			return "ap-southeast-1a";	//Singapore
		if (region.equalsIgnoreCase("ap-southeast-2"))
			return "ap-southeast-2a";	//Sydney
		if (region.equalsIgnoreCase("sa-east-1"))
			return "sa-east-1a";	//Sao Paulo
		return null;
	}

	//TODO generalize via EC2 api calls 
	public String getGroup(String region) {
		if (region.equalsIgnoreCase("us-east-1"))
			return "awtsrserver1";
		if (region.equalsIgnoreCase("us-west-1"))
			return "awtsrserver3";
		if (region.equalsIgnoreCase("us-west-2"))
			return "awtsrserver2";
		if (region.equalsIgnoreCase("eu-west-1"))
			return "awtsrserver4";
		if (region.equalsIgnoreCase("eu-central-1"))
			return "awtsrserver5";
		if (region.equalsIgnoreCase("ap-northeast-1"))
			return "awtsrserver6";
		if (region.equalsIgnoreCase("ap-southeast-1"))
			return "awtsrserver7";
		if (region.equalsIgnoreCase("ap-southeast-2"))
			return "awtsrserver8";
		if (region.equalsIgnoreCase("sa-east-1"))
			return "awtsrserver9";
		return null;
	}
	
	//TODO generalize via EC2 api calls 
	public String getSubnet(String region) {
		if (region.equalsIgnoreCase("us-east-1"))
			return "subnet-575b9520";
		if (region.equalsIgnoreCase("us-west-1"))
			return "subnet-d6c930b3";
		if (region.equalsIgnoreCase("us-west-2"))
			return "subnet-89699ffe";
		if (region.equalsIgnoreCase("eu-west-1"))
			return "subnet-f346f384";
		if (region.equalsIgnoreCase("eu-central-1"))
			return "subnet-d7837bbe";
		if (region.equalsIgnoreCase("ap-northeast-1"))
			return "subnet-b722e9c0";
		if (region.equalsIgnoreCase("ap-southeast-1"))
			return "subnet-bb0afecc";
		if (region.equalsIgnoreCase("ap-southeast-2"))
			return "subnet-30887447";
		if (region.equalsIgnoreCase("sa-east-1"))
			return "subnet-1bbe5b6c";
		return null;
	}
	
}
