package com.squigglee.test;


import org.junit.Test;

import com.squigglee.core.algorithm.Bijection;
import java.math.BigInteger;
import java.util.*;


public class BijectionTest {
	static final Bijection bijection = new Bijection();
	
	@Test
	public void BijectionTest1() {
		List<Long> list1 = new ArrayList<Long>();
		list1.add(Long.parseLong("1"));
		list1.add(Long.parseLong("1"));		
		BigInteger bg1 = bijection.getBijection(list1);
		//System.out.println("x = " + list.get(0) + "; y = " + list.get(1) + "; bijection = " + bg.toString());
		assert bg1.toString().equals("3");

		List<Integer> list2 = new ArrayList<Integer>();
		list2.add(Integer.parseInt("100"));
		list2.add(Integer.parseInt("300"));		
		BigInteger bg2 = bijection.getBijection(list2);
		//System.out.println("x = " + list.get(0) + "; y = " + list.get(1) + "; bijection = " + bg.toString());
		assert bg2.toString().equals("90100");	
		
		List<Short> list3 = new ArrayList<Short>();
		list3.add(Short.parseShort("11"));
		list3.add(Short.parseShort("12"));		
		BigInteger bg3 = bijection.getBijection(list3);
		//System.out.println("x = " + list.get(0) + "; y = " + list.get(1) + "; bijection = " + bg.toString());
		assert bg3.toString().equals("155");	
		
	}

	@Test
	public void BijectionTest2() {
		List<Long> list = new ArrayList<Long>();
		list.add(Long.parseLong("1"));
		list.add(Long.parseLong("1"));
		list.add(Long.parseLong("1"));
		BigInteger bg = bijection.getBijection(list);
		//System.out.println("x1 = " + list.get(0) + "; x2 = " + list.get(1) + "; x3 = " + list.get(2) + "; bijection = " + bg.toString());
		assert bg.toString().equals("13");
		
		list = new ArrayList<Long>();
		list.add(Long.parseLong("3"));
		list.add(Long.parseLong("7"));
		list.add(Long.parseLong("5"));
		bg = bijection.getBijection(list);
		//System.out.println("x1 = " + list.get(0) + "; x2 = " + list.get(1) + "; x3 = " + list.get(2) + "; bijection = " + bg.toString());
		assert bg.toString().equals("2761");
	}

	@Test
	public void ProjectionTest1() throws Exception {
		List<Long> list = new ArrayList<Long>();
		list.add(Long.parseLong("3"));
		list.add(Long.parseLong("1"));
		BigInteger bg = bijection.getBijection(list);
		BigInteger first = bijection.getProjection(1, bg);
		BigInteger second = bijection.getProjection(2, bg);
		//System.out.println("For bijection = " + bg.toString() + " -- first value = " + first + " and second value = " + second);
		assert first.compareTo(new BigInteger(list.get(0)+"")) == 0;
		assert second.compareTo(new BigInteger(list.get(1)+"")) == 0;		
	}
	
	@Test
	public void ProjectionTest2() throws Exception {
		List<Long> list = new ArrayList<Long>();
		list.add(new Long(Short.MAX_VALUE));
		list.add(new Long(Short.MAX_VALUE));
		BigInteger bg = bijection.getBijection(list);
		BigInteger first = bijection.getProjection(1, bg);
		BigInteger second = bijection.getProjection(2, bg);
		//System.out.println("For bijection = " + bg.toString() + " -- first value = " + first + " and second value = " + second);
		assert first.compareTo(new BigInteger(list.get(0)+"")) == 0;
		assert second.compareTo(new BigInteger(list.get(1)+"")) == 0;
		
		list = new ArrayList<Long>();
		list.add(new Long(Integer.MAX_VALUE));
		list.add(new Long(Integer.MAX_VALUE));
		bg = bijection.getBijection(list);
		first = bijection.getProjection(1, bg);
		second = bijection.getProjection(2, bg);
		//System.out.println("For bijection = " + bg.toString() + " -- first value = " + first + " and second value = " + second);
		assert first.compareTo(new BigInteger(list.get(0)+"")) == 0;
		assert second.compareTo(new BigInteger(list.get(1)+"")) == 0;

		list = new ArrayList<Long>();
		list.add(new Long(Long.MAX_VALUE));
		list.add(new Long(Long.MAX_VALUE));
		bg = bijection.getBijection(list);
		first = bijection.getProjection(1, bg);
		second = bijection.getProjection(2, bg);
		//System.out.println("For bijection = " + bg.toString() + " -- first value = " + first + " and second value = " + second);
		assert first.compareTo(new BigInteger(list.get(0)+"")) == 0;
		assert second.compareTo(new BigInteger(list.get(1)+"")) == 0;
	}
	
	@Test
	public void ProjectionTest3() throws Exception {
		List<Long> list = new ArrayList<Long>();
		int variables=5;
		for (int i=0; i< variables; i++)
			list.add(Long.MAX_VALUE);
		
		BigInteger bg = bijection.getBijection(list);
		System.out.println("Bijection for " + variables + " variables of max size " +  list.get(0) + " = " + bg.toString());
		System.out.println("Max radix = " + Character.MAX_RADIX);
		System.out.println(bg.toString(Character.MAX_RADIX));

		for (int i=0; i< variables; i++) {
			BigInteger result = bijection.getProjection(variables,(i+1), bg);
			System.out.println(result);
			assert bijection.getProjection(variables,(i+1), bg).compareTo(BigInteger.valueOf(list.get(0).longValue())) == 0;
		}
	}

	@Test
	public void ProjectionTest4() throws Exception {
		List<Long> list = new ArrayList<Long>();
		list.add(Long.parseLong("3"));
		list.add(Long.parseLong("7"));
		list.add(Long.parseLong("5"));
		BigInteger bg = bijection.getBijection(list);
		//System.out.println("x1 = " + 1 + "; x2 = " + 1 + "; x3 = " + 1 + "; bijection = " + bg.toString());
		BigInteger first = new BigInteger("0");
		BigInteger second = new BigInteger("0");
		BigInteger third = new BigInteger("0");
		try {
			first = bijection.getProjection(3, 1, bg);
			second = bijection.getProjection(3, 2, bg);
			third = bijection.getProjection(3, 3, bg);
		} catch (IllegalArgumentException e) {
			assert "Projection index should be non-zero and less than number bijected".equals(e.getMessage());
		}
		//System.out.println("For bijection = " + bg.toString() + " -- first value = " + first + " and second value = " 
		//+ second+ " and third value = " + third);
		assert first.compareTo(new BigInteger(list.get(0)+"")) == 0; 
		assert second.compareTo(new BigInteger(list.get(1)+"")) == 0;
		assert third.compareTo(new BigInteger(list.get(2)+"")) == 0;
	}
	
	@Test
	public void LargerNumberProjectionTest() throws Exception {
		List<BigInteger> list = new ArrayList<BigInteger>();
		BigInteger a = BigInteger.valueOf(Long.MAX_VALUE);
		BigInteger b = BigInteger.valueOf(Long.MAX_VALUE);
		for ( int i = 0; i< 10 ; i++) {
			a = a.multiply(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(Math.round(Math.random()*11111))));
			b = b.multiply(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(Math.round(Math.random()*22222))));
		}
		list.add(a);
		list.add(b);
		BigInteger bg = bijection.getBijectionExtended(list);
		BigInteger first = bijection.getProjection(1, bg);
		BigInteger second = bijection.getProjection(2, bg);
		//System.out.println("For bijection = \n" + bg.toString() + "\n -- first value = \n" + first + "\n and second value = \n" + second);
		assert first.compareTo(a) == 0;
		assert second.compareTo(b) == 0;
	}
}
