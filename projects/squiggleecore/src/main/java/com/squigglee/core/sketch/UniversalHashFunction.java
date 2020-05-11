// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.sketch;

//import java.math.BigInteger;

public class UniversalHashFunction implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7970011406200883119L;
	//private BigInteger p = null;
	//private BigInteger m = null;
	//private BigInteger a = null;
	//private BigInteger b = null;
	//private long prime, reducedSize, scalar1, scalar2;
	private int p;
	private int m;
	private int a;
	private int b;
//	private int d = 8;
	//short s = (short) Random.nextInt(Short.MAX_VALUE + 1);
	
	public UniversalHashFunction(int prime, int reducedSize, int scalar1, int scalar2) {
		//p = new BigInteger("" + prime);
		//a = new BigInteger("" + scalar1);
		//b = new BigInteger("" + scalar2);
		//m = new BigInteger("" + reducedSize);
		
		p = prime;
		a = scalar1;
		b = scalar2;
		m = reducedSize;
		//this(prime, reducedSize, scalar1, scalar2, 8);
	}
	
//	public UniversalHashFunction(int prime, int reducedSize, int scalar1, int scalar2, int digits) {
		//p = new BigInteger("" + prime);
		//a = new BigInteger("" + scalar1);
		//b = new BigInteger("" + scalar2);
		//m = new BigInteger("" + reducedSize);
//		p = prime;
//		a = scalar1;
//		b = scalar2;
//		m = reducedSize;
//		d = digits;
//	}	
	
	public UniversalHashFunction() {}
	
	//public int hash(int value) {
	//	return (new BigInteger("" + value)).multiply(a).add(b).mod(p).mod(m).intValue();
	//}

	public int hash(int value) {
		return (int) ( ((( (long) a) * value + b) % p) % m);
	}
	
	/*
	public int hash(int value, int order) {
		BigInteger bg = (new BigInteger("" + value)).multiply(new BigInteger("" + a));
		//BigInteger m1 = m.divide(new BigInteger("" + order));
		BigInteger m1 = m.divide(new BigInteger("" + 1));
		return (new BigInteger("" + value)).multiply(a).add(b).mod(p).mod(m1).intValue();
		//return (new BigInteger("" + value)).multiply(a).add(b).mod(p).mod(m).intValue();
		//return bg.mod(new BigInteger("" + p)).mod(new BigInteger("" + m)).intValue();
		//return ((a*value + b) % p ) % m;
		//long tmp = value;
		//if (value < 0)
		//	value += Integer.MAX_VALUE + value + 1;
			
		//tmp = tmp*a + b;
		//return (int) ( tmp % p) % (m/order);
		//return ( (value*a + b) % p) % m/order;
		
	}
	*/
	
	//public int hash(double value) {
	//	return (new BigInteger("" + value*Math.pow(10, d))).multiply(a).add(b).mod(p).mod(m).intValue();
	//}
	
}