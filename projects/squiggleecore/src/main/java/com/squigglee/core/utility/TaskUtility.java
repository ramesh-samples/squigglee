// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.utility;

public class TaskUtility {

	public static boolean sleep(int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			System.out.println("Error while sleeping -- " + e.getMessage());
			return false;
		}
		return true;
	}
}
