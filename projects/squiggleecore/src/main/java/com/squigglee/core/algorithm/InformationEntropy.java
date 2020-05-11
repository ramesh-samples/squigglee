// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.algorithm;

public class InformationEntropy {

	public static double JensenShannonDivergence(double[] p1, double[] p2) {
		double[] q = new double[p1.length];
		for (int i = 0; i< q.length; i++)
			q[i] = (p1[i] + p2[i])/2.0;
		return ( KullbackLeiblerDivergence(p1,q) + KullbackLeiblerDivergence(p2,q) )/2.0;
	}
	
	//check to make sure sum of each vector p1 and p2 are within 1 to say 0.00001 or some small value 
	//handle duplicates correctly i.e.
	// handle edge conditions correctly , what if p == 0 or q == 0
	//output in nats i.e. logs are computed in base e
	public static double KullbackLeiblerDivergence(double[] p1, double[] p2) {
		double sum = 0.0;
		for ( int i = 0; i < p1.length; i++) {
			sum += p1[i] * (Math.log(p1[i]/p2[i]));
		}
		return sum;
	}
}
