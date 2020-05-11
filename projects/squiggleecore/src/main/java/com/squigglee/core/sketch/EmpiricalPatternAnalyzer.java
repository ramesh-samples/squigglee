package com.squigglee.core.sketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.stat.StatUtils;

public class EmpiricalPatternAnalyzer {
	private DistanceMeasure measure = null;
	private List<String> indexes = null;
	private int dimension, topk;
	private double radius;
	private long id;
	private List<Integer> data = null;
	double[][] vals, normvals;
	
	public EmpiricalPatternAnalyzer(long id, int dimension, double radius, int topk, List<Integer> data) {
		this.id = id;
		this.data = data;
		this.dimension = dimension;
		this.topk = topk;
		this.radius = radius;
		measure = new EuclideanDistance();
		indexes = new ArrayList<String>();
		System.out.println("Created empirical pattern analyzer for data of size " + data.size() + " with search radius = " + radius 
				+ " for at most top " + topk + " results for patterns of dimension " + dimension);
	}
	
	public Map<String,Stats> getResults() {
		
		Map<String,Stats> algorithmAccuracy = new HashMap<String,Stats>();
		for (String index : this.indexes) {
			System.out.println("Starting the calculations for index = " + index);
			LocalitySensitiveHasher lsh = getIndexer(index);
			//int counter = 0;
			Stats stats = new Stats();
			stats.reset();
			for (int i = 0; i < vals.length; i++) {
				//if (i != 0)
				//	continue;
				//counter++;
				List<Long> candidates = lsh.getIndexedNeighbors(normvals[i]);				
				SortedMap<Double,List<Long>> approxMatches = getApproximateMatches(normvals[i], candidates);
				SortedMap<Double,List<Long>> trueMatches = getTrueMatches(normvals[i]);
				List<Long> indexedMatches = getTopkMatches(approxMatches, topk);
				List<Long> exactMatches = getTopkMatches(trueMatches, topk);
				int tp = 0;
				int fn = 0;
				int fp = 0;
				int tn = 0;
				for (Long em : exactMatches) {
					if (indexedMatches.contains(em))
						tp++;		//true positive
					else
						fn++;		// false negative 
				}
				for (Long im : indexedMatches) {
					if (!exactMatches.contains(im))
						fp++;		// false positive 
				}
				for (int n = 0; n < vals.length; n++) {
					if (!exactMatches.contains(new Long(n)))
						tn++;		// these are the true negatives 
				}
				stats.add(tp, tn, fp, fn);
				if (i > 0 && i % 20000 == 0)
					System.out.println("Completed results upto index position = " + i);
			}
			algorithmAccuracy.put(index, stats);
			System.out.println("Completed the calculations for index = " + index + " with stats = " + stats);
		}
		return algorithmAccuracy;
	}
	
	public void addParameter(int dimension, int bucketWidth, int projections, int size, int scalar) {
		this.indexes.add("ptrn" + "_" + dimension + "_" + bucketWidth + "_" + projections + "_" + size + "_" + scalar);
	}
	
	public void addParameter(String index) {
		this.indexes.add(index);
	}
	
	public void addParameter(List<String> indexes) {
		this.indexes.addAll(indexes);
	}
	
	private LocalitySensitiveHasher getIndexer(String index) {
		LocalitySensitiveHasher lsh = new LocalitySensitiveHasher(1,0);
		lsh.setValuesFromTableName(this.id, index);
		for (int i = 0; i< this.vals.length; i++)
				lsh.index(i, this.vals[i]);
	
		return lsh;
	}
	
	private SortedMap<Double,List<Long>> getApproximateMatches(double[] pattern, List<Long> candidates) {
		SortedMap<Double,List<Long>> matches = new TreeMap<Double, List<Long>>();
		for (long c : candidates) {
			double dist = this.measure.compute(pattern, normvals[(int) c]);
			if (dist > this.radius)
				continue;
			if (!matches.containsKey(dist))
				matches.put(dist, new ArrayList<Long>());
			matches.get(dist).add(c);
		}
		return matches;
	}
	
	private SortedMap<Double,List<Long>> getTrueMatches(double[] pattern) {
		SortedMap<Double,List<Long>> matches = new TreeMap<Double, List<Long>>();
		for (int c = 0; c< vals.length; c++) {
			double dist = this.measure.compute(pattern, normvals[c]);
			if (dist > this.radius)
				continue;
			if (!matches.containsKey(dist))
				matches.put(dist, new ArrayList<Long>());
			matches.get(dist).add((long) c);
		}
		return matches;
	}
	
	private List<Long> getTopkMatches(SortedMap<Double,List<Long>> matches, int topk) {
		List<Long> topkMatches = new ArrayList<Long>();
		for (double dist : matches.keySet()) {
			for (Long c : matches.get(dist)) {
				if (topkMatches.size() < topk)
					topkMatches.add(c);
				else
					return topkMatches;
			}
		}
		return topkMatches;
	}
		
	public void reset() {
		this.indexes.clear();
	}
	
	public void initialize() {
		vals = new double[data.size() - dimension + 1][];
		normvals = new double[data.size() - dimension + 1][];
		for (int i = 0; i < vals.length; i++) {
			vals[i] = new double[dimension];
			for (int j=0; j< vals[i].length; j++) {
				vals[i][j] = data.get(i + j);
			}
			normvals[i] = StatUtils.normalize(vals[i]);
		}
		System.out.println("Initialized data arrays");		
	}
	
	public class Stats {
		
		public long getCount() { return this.count;}
		public double getPrecision() { return (double) Math.round( (this.tp)*1.0/(this.tp + this.fp) * 1000000.0) / 1000000.0;}	 
		public double getRecall() { return (double) Math.round( (this.tp)*1.0/(this.tp + this.fn) * 1000000.0) / 1000000.0;}	 
		public double getAccuracy() { return (double) Math.round( (this.tp + this.tn)*1.0/(this.tp +  this.tn + this.fp + this.fn) * 1000000.0) / 1000000.0;}	 
		
		private long count;
		private long tp,tn,fp,fn;
		
		public void reset() {
			count = tp = tn = fp = fn = 0;
		}
		
		public void add(int tp, int tn, int fp, int fn) {
			count++;
			this.tp += tp;
			this.tn += tn;
			this.fp += fp;
			this.fn += fn;
		}
		
	}
}
