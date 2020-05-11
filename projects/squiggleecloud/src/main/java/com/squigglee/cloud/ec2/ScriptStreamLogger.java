package com.squigglee.cloud.ec2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class ScriptStreamLogger extends Thread {
	private static Logger logger = Logger.getLogger("com.squigglee.cloud.ec2.ScriptStreamLogger");
	private InputStream is = null;
	private StreamType type = null;
	private ScriptType script = null;
	
	public ScriptStreamLogger(InputStream is, StreamType type, ScriptType script) {
		this.is = is;
		this.type = type;
		this.script = script;
	}
	
	public void run()
    {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ( (line = br.readLine()) != null) {
                System.out.println(script + ">" + type + ">>>" + line);
                if (type.equals(StreamType.ERROR))
                	logger.error(script + ">" + type + ">>>" + line);
                else
                	logger.debug(script + ">" + type + ">>>" + line);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();  
        }
    }
	
	public void close() throws IOException {
		if (is != null)
			is.close();
	}
}
