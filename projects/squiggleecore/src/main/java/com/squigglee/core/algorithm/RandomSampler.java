// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.algorithm;

import java.util.ArrayList;
import java.util.List;

import com.squigglee.core.entity.TimeSeriesException;

public class RandomSampler {

	//Classic method per Knuth
	//TODO switch to arrays everywhere 
	//maybe add startts as parameter later 
	public List<Long> sampleWithoutReplacementS(long sampleSize, long dataSize) throws TimeSeriesException {
		if (sampleSize > dataSize)
			throw new TimeSeriesException("Requested sample size is greater than data size [" + sampleSize + "," + dataSize + "]");
		List<Long> list = new ArrayList<Long>();
		long N = dataSize;
		long n = sampleSize;
			
		//for (long i = 0; i< dataSize; i++) {
		/*
		while (true) {
			double U = Math.random();
			if (N*U > n) {
				N--;
			}
			else {
				list.add(0,N);
				n--;
				N--;
			} 
			if (n <= 0)
				break;
		}
		*/
		while (n > 0) {
			if ( N*Math.random() <= n ) {
				list.add(0, new Long(N-1));
				n--;
			}
			N--;
		}
		return list;
	}
	
	//TODO faster Rejection method per Vitter
	public List<Long> sampleWithoutReplacementD(long sampleSize, long dataSize) {
		List<Long> list = new ArrayList<Long>();
		
		
		
		return list;
	}
}
