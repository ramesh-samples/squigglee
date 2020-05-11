// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.coord.utility;

import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class AuthenticationUtility {
	private static Logger logger = Logger.getLogger("com.squigglee.coord.zk.AuthenticationUtility");
	public static String getDigestPassword(String password) {
		try {
			return org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest(password) ;
		} catch (NoSuchAlgorithmException e) {
			logger.error("Error generating digest password", e);
		}
		return null;
	}
}
