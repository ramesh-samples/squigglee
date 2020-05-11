package com.squigglee.api.rest.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.squigglee.api.rest.IPatternRESTService;
import com.squigglee.core.entity.Matches;
import com.squigglee.core.entity.Pattern;
import com.squigglee.core.entity.TimeSeriesException;
import com.squigglee.core.interfaces.HandlerFactory;

public class PatternRESTService extends RestBase implements IPatternRESTService {
	private static Logger logger = Logger.getLogger("com.squigglee.api.rest.StatusRESTService");
	protected static ExecutorService executorService = null;
	
	@Override
	public Matches fetchMatchesJSON(Matches request) {
		logger.debug("Received request for matches for pattern " + request.getPattern() + " for configs " + 
				request.getRequestDomain() + " for radius " + request.getRadius() + " for top " + 
				request.getTopk() + " results");
		System.out.println("Received request for matches for pattern " + request.getPattern() + " for configs " + 
				request.getRequestDomain() + " for radius " + request.getRadius() + " for top " + 
				request.getTopk() + " results");
		request = patternProxy.executePatternMatchSplits(request);
		System.out.println("Matches = " + request.getMatchResults());
		return request;
	}
	
	@Override
	public Pattern fetchStoredPatternJSON(String cluster, String pguid) {
		logger.debug("Received request to fetch stored pattern with pguid " + pguid + " in cluster " + cluster);
		System.out.println("Received request to fetch stored pattern with pguid " + pguid + " in cluster " + cluster);
		Pattern pattern = new Pattern(cluster, pguid);
		if (pattern != null && cluster != null && pguid != null) {
			logger.debug("Received request for fetching stored pattern " + pguid + " in cluster " + cluster);
			System.out.println("Received request for fetching stored pattern " + pguid + " in cluster " + cluster);
			try {
				pattern.setVals(HandlerFactory.getPatternHandler().fetchPattern(cluster, pguid));
				logger.debug("Completed request to fetch stored pattern with pguid " + pguid + " in cluster " + cluster);
				System.out.println("Completed request to fetch stored pattern with pguid " + pguid + " in cluster " + cluster);
				pattern.setErrorMessage(null);
			} catch (TimeSeriesException e) {
				logger.error("Found error fetching stored pattern for cluster = " + cluster + " and pguid = " + pguid, e);
				pattern.setErrorMessage(e.getMessage());
			}
		} else {
			pattern.setErrorMessage("Cluster and pattern guid must be set for request");
		}
		return pattern;
	}

	@Override
	public Pattern storePatternJSON(Pattern pattern) {
		logger.debug("Received request to store pattern with pguid " + pattern.getPguid() + " in cluster " + pattern.getCluster());
		System.out.println("Received request to store pattern with pguid " + pattern.getPguid() + " in cluster " + pattern.getCluster());
		if (pattern != null && pattern.getCluster() != null && pattern.getPguid() != null && pattern.getVals() != null && pattern.getVals().size() > 0) {
			try {
				HandlerFactory.getPatternHandler().storePattern(pattern.getCluster(), pattern.getPguid(), pattern.getVals());
				logger.debug("Completed request to store pattern with pguid " + pattern.getPguid() + " in cluster " + pattern.getCluster());
				System.out.println("Completed request to store pattern with pguid " + pattern.getPguid() + " in cluster " + pattern.getCluster());
				pattern.setErrorMessage(null);
			} catch (TimeSeriesException e) {
				logger.error("Found error fetching stored pattern for cluster = " + pattern.getCluster() + " and pguid = " + pattern.getPguid() + " and pattern = " + pattern, e);
				pattern.setErrorMessage(e.getMessage());
			}
		}
		return pattern;
	}

	@Override
	public List<Pattern> fetchCapturedPatternsJSON(String cluster) {
		logger.debug("Received request for fetching all patterns in cluster " + cluster);
		System.out.println("Received request for fetching all patterns in cluster " + cluster);
		List<Pattern> list = new ArrayList<Pattern>();
		try {
			List<String> patternNames = HandlerFactory.getPatternHandler().fetchCapturedPatterns(cluster);
			for (String pguid : patternNames)
				list.add(new Pattern(cluster, pguid));
			logger.debug("Completed request for fetching all patterns in cluster " + cluster);
			System.out.println("Completed request for fetching all patterns in cluster " + cluster);
		} catch (TimeSeriesException e) {
			logger.error("Found error fetching stored pattern for cluster = " + cluster, e);
			Pattern err = new Pattern(cluster, null, null);
			err.setErrorMessage(e.getMessage());
			list.add(err);
		}
		return list;
	}

}
