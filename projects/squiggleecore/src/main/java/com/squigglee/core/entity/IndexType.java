// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.core.entity;

public enum IndexType {
	ptrn,skchCM, skchEX;
	
	public static final IndexType parseIndexTableName(String tableName) {
		String[] vals = tableName.split("_");
		return IndexType.valueOf(vals[0]);
	}
}

