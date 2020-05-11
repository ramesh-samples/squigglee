package com.squigglee.client.cl;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommandLineHelper {

	public static Options getAllOptions() {
		Options options = new Options();
		// s = new Option( "server", "Node Server Address" );
		Option s = new Option("s","server",true,"Node Server Address");
		Option p = new Option("p","port",true,"Node server port");
		Option u = new Option("u","user",true,"Node server user");
		Option c = new Option("c","userpw",true,"Node server password/credential");
		Option i = new Option("i","id",true,"Time series identifier");
		Option l = new Option("l","ln",true,"Logical number");
		Option t = new Option("t","cluster",true,"Cluster");
		Option b = new Option("b","start",true,"Start date, yyyy-MM-dd'T'HH:mm:ss.SSSZ format e.g. 2014-09-09T00:00:00.000-0000");
		Option e = new Option("e","end",true,"End date, yyyy-MM-dd'T'HH:mm:ss.SSSZ format e.g. 2014-09-09T00:00:00.000-0000");
		Option f = new Option("f","freq",true,"Time series frequency, Hz");
		Option d = new Option("d","datatype",true,"Time series data type");
		Option z = new Option("z","batchsize",true,"Insertion batch size for bulk data");
		Option r = new Option("r","file",true,"Csv file for batch inserts");
		Option m = new Option("m","method",true,"Method for random data inserts instead of from file");
		Option x = new Option("x", "pause", true, "Pauses for x seconds after each batch before resuming");
		Option o = new Option("o", "offset", true, "Start offset for resumption of random data load");
		Option h = new Option("h", "help", false, "Prints the help for the command line interface");
		
		options.addOption(s);
		options.addOption(p);
		options.addOption(u);
		options.addOption(c);
		options.addOption(i);
		options.addOption(l);
		options.addOption(b);
		options.addOption(e);
		options.addOption(f);
		options.addOption(d);
		options.addOption(z);
		options.addOption(r);
		options.addOption(m);
		options.addOption(x);
		options.addOption(o);
		options.addOption(t);
		options.addOption(h);
		return options;
	}
}
