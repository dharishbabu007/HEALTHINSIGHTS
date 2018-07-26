/**
 * 
 */
package com.qms.rest.exception;

public class QmsInsertionException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public QmsInsertionException(String errorMessage) {
        super(errorMessage);
    }

    public QmsInsertionException(String errorMessage, Exception e) {
        super(errorMessage, e);
    }
}
