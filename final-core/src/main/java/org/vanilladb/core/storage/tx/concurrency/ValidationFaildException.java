package org.vanilladb.core.storage.tx.concurrency;

@SuppressWarnings("serial")
public class ValidationFaildException extends Exception {
	public ValidationFaildException() {
		super();
	}
	
	public ValidationFaildException(String message){
		super(message);
	}
}
