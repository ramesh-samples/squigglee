// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.synthetic;

public class Random {

	//=========================================================================
	//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
	//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
	//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
	//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
	//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
	//=========================================================================
	public static double rand_val(int seed)
	{
	  long a =      16807;  // Multiplier
	  long m = 2147483647;  // Modulus
	  long q =     127773;  // m div a
	  long r =       2836;  // m mod a
	  long x = 0;               // Random int value
	  long x_div_q;         // x divided by q
	  long x_mod_q;         // x modulo q
	  long x_new;           // New x value

	  // Set the seed if argument is non-zero and then return zero
	  if (seed > 0)
	  {
	    x = seed;
	    return(0.0);
	  }

	  // RNG using integer arithmetic
	  x_div_q = x / q;
	  x_mod_q = x % q;
	  x_new = (a * x_mod_q) - (r * x_div_q);
	  if (x_new > 0)
	    x = x_new;
	  else
	    x = x_new + m;

	  // Return a random value between 0.0 and 1.0
	  return((double) x / m);
	}
	
}
