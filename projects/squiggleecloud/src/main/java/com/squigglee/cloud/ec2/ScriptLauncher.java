package com.squigglee.cloud.ec2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.TimeSeriesException;

public class ScriptLauncher {
	private static Logger logger = Logger.getLogger("com.squigglee.cloud.ec2.ScriptLauncher");
	
	public int Launch(ScriptType script, String parms) {
		String command = null;
		try {
			command = getScript(script);
			if (command == null)
				throw new TimeSeriesException("Script command is not yet supported -- " + script);
			String[] env = new String[]{
					"ANSIBLE_HOSTS=" + LocalNodeProperties.getScriptLocation() + "/hosts",
//					"ANSIBLE_HOSTS=" + "/Users/AgnitioWorks/Documents/tsr/ansible" + "/hosts",
//					"PATH=/usr/local/bin:/usr/local/sbin:/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/ec2/ec2-api-tools-1.7.1.0/bin",
					"PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin:/home/ec2-user/bin:/usr/java/jdk1.7.0_67/bin",
					"AWS_ACCESS_KEY=AKIAIQWKLMSGKA7YCSLA",
					"AWS_SECRET_KEY=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",
					"EC2_HOME=/usr/local/ec2/ec2-api-tools-1.7.1.0",
					"EC2_INI_PATH=" + LocalNodeProperties.getScriptLocation() + "/hosts/ec2.ini",
					"SSH_AUTH_SOCK=/private/tmp/com.apple.launchd.tK7OUt4r7N/Listeners",
//					"BOTO_PATH=~/.boto",
//					"EC2_INI_PATH=/Users/AgnitioWorks/Documents/tsr/ansible/ec2.ini"
//					"HOME=/Users/AgnitioWorks",
//					"EC2_URL=https://ec2.us-east-1.amazonaws.com",
					"ANSIBLE_CONFIG=" + LocalNodeProperties.getScriptLocation() + "/ansible.cfg"
			};
			//Process proc = Runtime.getRuntime().exec(command, env, new File(LocalNodeProperties.getScriptLocation()));
			logger.debug("Launching script file = " + command + " with --extra-vars = " + parms);
			Process proc = null;
			if (parms != null && parms.length() > 0)
				proc = Runtime.getRuntime().exec(new String[]{LocalNodeProperties.getAnsibleExe(),command,"--extra-vars",parms}, 
					env, new File(LocalNodeProperties.getScriptLocation()));
			else
				proc = Runtime.getRuntime().exec(new String[]{LocalNodeProperties.getAnsibleExe(),command}, 
						env, new File(LocalNodeProperties.getScriptLocation()));
			
			ScriptStreamLogger errorLogger = new ScriptStreamLogger(proc.getErrorStream(), StreamType.ERROR, script);
			ScriptStreamLogger outputLogger = new ScriptStreamLogger(proc.getInputStream(), StreamType.OUT, script);
			errorLogger.start();
			outputLogger.start();
			int exitValue = proc.waitFor();
			if (proc != null && proc.getInputStream() != null)
				proc.getInputStream().close();
			if (proc != null && proc.getErrorStream() != null)
				proc.getErrorStream().close();			
			//int exitValue = 0;
			System.out.println("Completed script request for " + command + " with status = " + exitValue);
			logger.info("Completed script request for " + command + " with status = " + exitValue);
			
			return exitValue;
		} catch (Throwable t) {
			logger.error("Failed script execution for command " + command,t);
			return -1;
		}
	}
	
	private String getScript(ScriptType script) throws TimeSeriesException {
		switch(script) {
			case CONF_CASS:
				return "tsr_ec2_conf_cass.yml";
			case CONF_JBOSS:
				return "tsr_ec2_conf_jboss";
			case CONF_PREREQ:
				return "tsr_ec2_conf_prereq.yml";
			case CONF_TSR:
				return "tsr_ec2_conf_tsr.yml";
			case CONF_VOLS:
				return "tsr_ec2_conf_vols.yml";
			case CONFIGURE:
				return "tsr_ec2_configure.yml";
			case CREATE_TOPOLOGY:
				return "tsr_ec2_create_topology.yml";
			case DELETE_TOPOLOGY:
				return "tsr_ec2_delete_topology.yml";
			case SET_SCHEMA:
				return "tsr_ec2_setschema.yml";
			case START_ALL:
				return "tsr_ec2_startall.yml";
			case START_NODE:
				return "tsr_ec2_startnode.yml";
			case START_SERVICES_ALL:
				return "tsr_ec2_services_startall.yml";
			case START_SERVICES_NODE:
				return "tsr_ec2_services_startnode.yml";
			case STOP_SERVICES_NODE:
				return "tsr_ec2_services_stopnode.yml";
			case START_SERVICES_COORD:
				return "tsr_ec2_startzkall.yml";
			default:
				break;
		}
		return null;
	}
	
	public static void main (String[] args) {
		List<Map<String, String>> topology = new ArrayList<Map<String, String>>();
		Map<String,String>	map = new HashMap<String,String>();
		map.put("ln","1");
		map.put("cluster", "Cluster1");
		map.put("dc","us-east-1");
		map.put("stype","Medium");
		map.put("storage","10");
		map.put("isReplica","true");
		map.put("isSeed","false");
		map.put("isBoot","false");
		topology.add(map);
		
		StringBuilder builder = new StringBuilder("topology=[");
		EC2Mapper mapper = new EC2Mapper();
		for (Map<String,String> node : topology) {
			builder.append("{");
			builder.append("\"cluster\":\"" + mapper.getType(node.get("cluster")) + "\",");
			builder.append("\"instance_type\":\"" + mapper.getType(node.get("stype")) + "\",");
			builder.append("\"image\":\"" + mapper.getImage(node.get("dc")) + "\",");
			builder.append("\"group\":\"" + mapper.getGroup(node.get("dc")) + "\",");
			builder.append("\"region\":\"" + node.get("dc") + "\",");
			builder.append("\"zone\":\"" + mapper.getZone(node.get("dc")) + "\",");
			builder.append("\"vpc_subnet_id\":\"" + mapper.getSubnet(node.get("dc")) + "\",");
			builder.append("\"token\":\"" + Long.parseLong(node.get("ln"))*1000000000 + "\",");
			builder.append("\"ring_position\":\"" + node.get("ln") + "\",");
			builder.append("\"is_seed\":\"" + (Boolean.parseBoolean(node.get("isSeed"))?"yes":"no") + "\",");
			builder.append("\"is_bootstrap_node\":\"" + (Boolean.parseBoolean(node.get("isBoot"))?"yes":"no") + "\",");
			builder.append("\"Name\":\"" + "TimeSeriesNode_" + node.get("ln") + "\",");
			builder.append("\"replica_of\":\"" + "0" + "\"");
			builder.append("}");
		}
		builder.append("]");
		int result = (new ScriptLauncher()).Launch(ScriptType.CREATE_TOPOLOGY, builder.toString());
	
		System.out.println("Topology create status = " + result);
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
