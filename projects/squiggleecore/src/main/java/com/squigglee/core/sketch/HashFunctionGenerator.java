// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;

public class HashFunctionGenerator {
	
	public static UniversalHashFunction[] generatePairwiseUniversalHashFunctions(int size, int reducedSize, int count) {
		
		int prime = (new BigInteger(size + "")).nextProbablePrime().intValue();
		int[] scalar1Sample = sampleWithoutReplacement(count, prime - 1);
		int[] scalar2Sample = sampleWithoutReplacement(count, prime - 1);
		
		UniversalHashFunction[] hashers = new UniversalHashFunction[count];
		for (int i = 0; i < count ; i++) 
			hashers[i] = new UniversalHashFunction(prime, reducedSize, scalar1Sample[i], scalar2Sample[i]);
		
		return hashers;
	}
	
	public static VectorHashFunction[] generateStronglyUniversalHashFunctions(int count) 
			throws NoSuchAlgorithmException {
		VectorHashFunction[] hashers = new VectorHashFunction[count];
		for (int i = 0; i < count ; i++) 
			hashers[i] = new VectorHashFunction();
		return hashers;
	}
	
	/*
	public static Map<Long,VectorHashFunction[]> generateDyadicHashFunctions(int range, int count) 
			throws NoSuchAlgorithmException {
		Map<Long,VectorHashFunction[]> hashers = new HashMap<Long,VectorHashFunction[]>();
		int dyadicCount = (int) Math.ceil(Math.log(range));
		for (int dc = 0; dc < dyadicCount; dc++) {
			long order = (long) Math.pow(2, dc); 
			if (!hashers.containsKey(order))
				hashers.put(order, new VectorHashFunction[count]);
			for (int i = 0; i < count ; i++) { 
				hashers.get(dc)[i] = new VectorHashFunction();
			}
		}
		return hashers;
	}
	*/
	
	public static int[] sampleWithoutReplacement(int sampleSize, int dataSize) {
		int[] sample = new int[sampleSize];
		int N = dataSize;
		int n = sampleSize;
		while (n > 0) {
			if ( N*Math.random() <= n ) {
				sample[n-1] = N-1;
				n--;
			}
			N--;
		}
		return sample;
	}
}