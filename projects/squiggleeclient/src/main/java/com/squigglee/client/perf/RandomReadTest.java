package com.squigglee.client.perf;

import java.util.List;
import java.util.Random;
import java.util.SortedMap;

import org.joda.time.DateTime;

import com.squigglee.api.rest.IConfigRESTService;
import com.squigglee.api.rest.ITimeSeriesRESTService;
import com.squigglee.api.rest.RESTFactory;
import com.squigglee.core.config.LocalNodeProperties;
import com.squigglee.core.entity.MasterData;
import com.squigglee.core.entity.TimeSeriesConfig;
import com.squigglee.core.entity.TimeSeriesException;

public class RandomReadTest {
	private int testCount = 10000;
	private int fetchCount = 60000;
	private static final Random rand = new Random();
	private static long duration = 0L;
	private static final boolean[] order = (new boolean[]{true, false});
	private boolean verbose = true;
	private boolean nodeOnly = true;
	private String cluster = "TestCluster";
	private int ln = 0;
	
	static {
		RESTFactory.initialize("/squiggleerestui", 8080, "http", 20000, 20000);
	}
	
	public RandomReadTest() {
		this(10000,10000,false);
	}
	public RandomReadTest(int testCount) {
		this(testCount, 10000, false);
	}
	public RandomReadTest(int testCount, int fetchCount) {
		this(testCount, fetchCount, false);
	}
	public RandomReadTest(int testCount, int fetchCount, boolean verbose) {
		this.testCount = testCount;
		this.fetchCount = fetchCount;
		this.nodeOnly = false;
		this.verbose = verbose;
	}
	public RandomReadTest(String cluster, int ln) {
		this(cluster, ln, 10000,10000,false);
	}
	public RandomReadTest(String cluster, int ln, int testCount) {
		this(cluster, ln, testCount, 10000, false);
	}
	public RandomReadTest(String cluster, int ln, int testCount, int fetchCount) {
		this(cluster, ln, testCount, fetchCount, false);
	}
	public RandomReadTest(String cluster, int ln, int testCount, int fetchCount, boolean verbose) {
		this.cluster = cluster;
		this.ln = ln;
		this.testCount = testCount;
		this.fetchCount = fetchCount;
		this.nodeOnly = true;
		this.verbose = verbose;
	}
	
	public void runRandomReads(String addr) throws TimeSeriesException {
		List<TimeSeriesConfig> configs = null;
		IConfigRESTService configService = RESTFactory.getConfigurationProxy(addr);
		if (nodeOnly)
			configs = configService.getConfigJSON(cluster, ln);
		else	
			configs = configService.getGlobalConfigJSON();
		
		if (configs == null || configs.isEmpty())
			throw new TimeSeriesException("No data configuration in the system, aborting random read test");
		ITimeSeriesRESTService timeSeriesService = RESTFactory.getTimeSeriesProxy(addr);
		long count = 0L;
		for (int i =0; i< testCount; i++) {
			//rand.nextInt((max - min) + 1) + min
			TimeSeriesConfig config = configs.get(rand.nextInt(configs.size()));
			boolean last = order[rand.nextInt(order.length)];

			List<MasterData> mdList = configService.getMasterDataJSON(config.getCluster(), config.getLogicalNode(), config.getGuid(), 
					config.getStartDate().getMillis(), config.getEndDate().getMillis());
	
			if (mdList == null || mdList.isEmpty())
				throw new TimeSeriesException("No master data in the system for config " + config + ", aborting random read test");
			MasterData md = mdList.get(rand.nextInt(mdList.size()));
			
			String[] currentStatus = configService.getMasterDataStatusJSON(md).split(";");
			long startOffset = Long.parseLong(currentStatus[0]);	//0
			long endOffset = Long.parseLong(currentStatus[1]);	//999
				if ((endOffset - startOffset + 1) > fetchCount) {	//100
					if (last) {
						endOffset = fetchCount + rand.nextInt((int) (endOffset - startOffset + 1 - fetchCount));	//100 + [0,900)
					}
					else
						startOffset = rand.nextInt((int) (endOffset - startOffset + 1 + fetchCount));	//[0,900)
				}
			long start = DateTime.now().getMillis();
			SortedMap<Long,Object> result =	timeSeriesService.getSequencedTimeSeriesJSON(md.getCluster(), md.getLn(), md.getGuid(), 
					md.getStartts(), (int) startOffset, (int) endOffset, fetchCount, last).getData();
			long iterDuration = DateTime.now().getMillis() - start;
			if (result == null || result.isEmpty()) {
				if (verbose)
					System.out.println("Completed iteration " + (i+1) + " in time " + iterDuration + " milliseconds but no data available for random query");
				continue;
			} else {				
				duration += iterDuration;
				if (verbose)
					System.out.println("Completed iteration " + (i+1) + " in time " + iterDuration + " milliseconds with fetched data count = " 
							+ (result==null?0:result.size()));
				count += result.size();
			}
		}
		System.out.println("Random read test result over " + (nodeOnly?"local data":"global data") + " at node " + 
				LocalNodeProperties.getNodeLogicalNumber() + " in cluster "	+ LocalNodeProperties.getClusterName() + 
				" = " + (duration*1.0 / testCount) + " millis latency and " + (count*1000 / duration) + " throughput for " 
				+ testCount + " runs each fetching " + fetchCount + " data points");
	}
	
	public static void main(String[] args) throws TimeSeriesException {
		String addr = "127.0.0.1";
		int testCount = 10000;
		int fetchCount = 10000;
		boolean local = true;
		boolean verbose = false;
		String nodeCluster = null;
		int nodeLn = 0;
		if (args.length > 0)
			addr = args[0];
		if (args.length > 1)
			testCount = Integer.parseInt(args[1]);
		if (args.length > 2)
			fetchCount = Integer.parseInt(args[2]);
		if (args.length > 3)
			verbose = Boolean.parseBoolean(args[3]);
		if (args.length > 4)
			local = Boolean.parseBoolean(args[4]);
		if (args.length > 5)
			nodeCluster = args[5];	
		if (args.length > 6)
			nodeLn = Integer.parseInt(args[6]);
		
		RandomReadTest tester = null;
		if (local)
			tester = new RandomReadTest(nodeCluster, nodeLn, testCount, fetchCount, verbose);
		else
			tester = new RandomReadTest(testCount, fetchCount, verbose);
		
		System.out.println("Starting Random Read Test at -- " + DateTime.now() + " for arguments " + args);
		tester.runRandomReads(addr);
		System.out.println("Completed Random Read Test at -- " + DateTime.now() + " for arguments " + args);
	}

}
