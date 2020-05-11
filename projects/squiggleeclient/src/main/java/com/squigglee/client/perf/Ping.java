package com.squigglee.client.perf;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

import com.squigglee.client.ClientException;
import com.squigglee.client.cl.CommandLineHelper;

public class Ping {
	
	static {
		try {
			Class.forName("org.teiid.jdbc.TeiidDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Teeid Driver Class Not Loaded");
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) throws ClientException {
		Options options = CommandLineHelper.getAllOptions();
		CommandLineParser parser = new BasicParser();
	    try {
	        // parse the command line arguments
	        CommandLine line = parser.parse( options, args );
	        String vdbName = "TIMESERIESGLOBAL";
	        String server = null;
	        if (line.hasOption("server"))
	        	server = line.getOptionValue("server");
	        else
	        	throw new ClientException("Server address must be specified");
	        String port = null;
	        if (line.hasOption("port"))
	        	port = line.getOptionValue("port");
	        else
	        	throw new ClientException("Server port must be specified");
	        String user = null;
	        if (line.hasOption("user"))
	        	user = line.getOptionValue("user");
	        else
	        	throw new ClientException("User must be specified");
	        String userpw = null;
	        if (line.hasOption("userpw"))
	        	userpw = line.getOptionValue("userpw");
	        else
	        	throw new ClientException("User password must be specified");
			String url = "jdbc:teiid:" + vdbName + "@mm://" + server + ":" + Integer.parseInt(port);
			
			Connection c1 =  DriverManager.getConnection(url, user, userpw);
			System.out.println("VDB Ping successful to url: " + c1.getMetaData().getURL());
			
			if (c1 != null && !c1.isClosed())
				c1.close();
	    }
	    catch( Exception exp ) {
	        throw new ClientException("Ping failed.  Reason: " + exp.getMessage(), exp);
	    }
	}
}
