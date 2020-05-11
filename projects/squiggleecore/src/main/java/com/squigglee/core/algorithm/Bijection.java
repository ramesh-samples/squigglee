package com.squigglee.core.algorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps Multidimensional Integer Space to Integer Space (NxNxN...N) --> N and back
 * Implementation of Elegant Pairing Function as per Matthew Szudzik (Wolfram Research)
 * Provides maximal mapping efficiency
 * Bijection(x,y) --> (y**2 + x) when (x != max(x,y)) else (x**2 + x + y)
 * Projections(z) --> when (z - (floor(sqrt(z))**2) < floor(sqrt(z))) 
 * 					 then pair --> [z-floor(sqrt(z))**2,floor(sqrt(z))] 
 * 					 else pair --> [floor(sqrt(z)),z - floor(sqrt(z))**2 - floor(sqrt(z))]  
 * Higher order implementation recursive -- bijection rolls-up list top down, projection unrolls list bottom up
 * @author h120538
 *
 */
public class Bijection {

	/**
	 * 
	 * @param vals -- numbers only -- longs, ints, shorts, bytes etc.
	 * @return
	 */
	public BigInteger getBijection(List<?> vals) {
		if (vals.size() <= 1)
			throw new IllegalArgumentException("At least two values must be provided for a bijection");
		List<BigInteger> bis = new ArrayList<BigInteger>();
		for (Object l : vals)
			bis.add(BigInteger.valueOf((new Long(l.toString()))));
		return getBijectionExtended(bis);
	}
	
	/**
	 * 
	 * @param vals -- arbitrarily large numbers
	 * @return
	 */
	public BigInteger getBijectionExtended(List<BigInteger> vals) {
		if (vals.size() <= 1)
			throw new IllegalArgumentException("At least two values must be provided for a bijection");
		List<BigInteger> valsCopy = new ArrayList<BigInteger>(vals);
		BigInteger left;
		BigInteger right;
		if (valsCopy.size() > 2 ) {
			right = valsCopy.remove(valsCopy.size()-1);
			left = getBijectionExtended(valsCopy);
		}
		else {
			left = valsCopy.remove(0);
			right = valsCopy.remove(0);
		}
		return getBijectionExtended(left,right);
	}

	public BigInteger getBijectionExtended(BigInteger a, BigInteger b) {
		BigInteger z = BigInteger.valueOf(0);
		BigInteger max = a.compareTo(b) >= 0 ? a : b; 
		if ((a.compareTo(max) == 0))
			z = ((a.multiply(a)).add(a)).add(b);
		else
			z = (b.multiply(b)).add(a);
		return z;		
	}
	
	/**
	 * 
	 * @param n -- dimensionality of bijection
	 * @param i -- index of desired projection component
	 * @param bg -- bijection of original list
	 * @return -- recovered value from bijection
	 * @throws IllegalArgumentException
	 */
	public BigInteger getProjection(int n, int i, BigInteger bg) throws IllegalArgumentException {
		if (i < 1 || i > n)
			throw new IllegalArgumentException("Projection index should be non-zero and less than number bijected");
		if (bg == null || bg.bitCount() <= 0)
			throw new IllegalArgumentException("Bijection provided is invalid or zero");
		
		if (n == 2 )
			return getProjection(i, bg);
		else {
			BigInteger left = getProjection(1,bg);
			BigInteger right = getProjection(2,bg);
			if (i==n)
				return right;
			else
				return getProjection(n-1,i,left);
		}
	}
	
	/**
	 * 
	 * @param i -- index number of bijection pair
	 * @param bg -- paired value bijection
	 * @return -- recovered pair value
	 * @throws IllegalArgumentException
	 */
	public BigInteger getProjection(int i, BigInteger bg) throws IllegalArgumentException {
		if (i > 2 || i < 1)
			throw new IllegalArgumentException("Projection index should be non-zero and less than two");
		if (bg == null || bg.bitCount() <= 0)
			throw new IllegalArgumentException("Bijection provided is invalid or zero");		
		BigInteger retVal;
		BigInteger flsq = bigIntSqRootFloor(bg);
		if (bg.subtract(flsq.multiply(flsq)).compareTo(flsq) < 0)
		{
			if (i == 1)
				retVal = bg.subtract(flsq.multiply(flsq)); 
			else
				retVal = flsq;
		} else {
			if (i == 1)
				retVal = flsq;
			else
				retVal =  bg.subtract(flsq.multiply(flsq)).subtract(flsq);
		}
		return retVal;
	}
	
	private BigInteger bigIntSqRootFloor(BigInteger x)
	        throws IllegalArgumentException {
	    if (x.compareTo(BigInteger.ZERO) < 0) {
	        throw new IllegalArgumentException("Negative argument.");
	    }
	    // square roots of 0 and 1 are trivial and
	    // y == 0 will cause a divide-by-zero exception
	    if (x == BigInteger.ZERO || x == BigInteger.ONE) {
	        return x;
	    } // end if
	    BigInteger two = BigInteger.valueOf(2L);
	    BigInteger y;
	    // starting with y = x / 2 avoids magnitude issues with x squared
	    for (y = x.divide(two);
	            y.compareTo(x.divide(y)) > 0;
	            y = ((x.divide(y)).add(y)).divide(two));
	    return y;
	} // end bigIntSqRootFloor

	/*
	private BigInteger bigIntSqRootCeil(BigInteger x)
	        throws IllegalArgumentException {
	    if (x.compareTo(BigInteger.ZERO) < 0) {
	        throw new IllegalArgumentException("Negative argument.");
	    }
	    // square roots of 0 and 1 are trivial and
	    // y == 0 will cause a divide-by-zero exception
	    if (x == BigInteger.ZERO || x == BigInteger.ONE) {
	        return x;
	    } // end if
	    BigInteger two = BigInteger.valueOf(2L);
	    BigInteger y;
	    // starting with y = x / 2 avoids magnitude issues with x squared
	    for (y = x.divide(two);
	            y.compareTo(x.divide(y)) > 0;
	            y = ((x.divide(y)).add(y)).divide(two));
	    if (x.compareTo(y.multiply(y)) == 0) {
	        return y;
	    } else {
	        return y.add(BigInteger.ONE);
	    }
	} // end bigIntSqRootCeil
	*/
}