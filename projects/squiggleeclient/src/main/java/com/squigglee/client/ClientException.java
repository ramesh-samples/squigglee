package com.squigglee.client;

public class ClientException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ClientException(String message) {
		        super(message);
		    }
	
	public ClientException(String msg, Throwable t) {
		super(msg, t);
	}
	
	public ClientException(Throwable t) {
		super(t);
	}
}
