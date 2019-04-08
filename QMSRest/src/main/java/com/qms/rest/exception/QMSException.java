/**
 * 
 */
package com.qms.rest.exception;

public class QMSException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public QMSException(String errorMessage) {
        super(errorMessage);
    }

    public QMSException(String errorMessage, Exception e) {
        super(errorMessage, e);
    }
}
