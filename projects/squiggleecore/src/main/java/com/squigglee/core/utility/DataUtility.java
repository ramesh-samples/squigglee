package com.squigglee.core.utility;

public class DataUtility {
	public static int getByteSize(String dataType) {
		switch (dataType) {
		case "int":
			return 4;
		case "double":
			return 8;
		case "long":
			return 8;
		}
		
		return 4;
	}
}
