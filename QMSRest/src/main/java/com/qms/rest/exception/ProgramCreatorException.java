package com.qms.rest.exception;

public class ProgramCreatorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ProgramCreatorException(String errorMessage) {
        super(errorMessage);
    }

    public ProgramCreatorException(String errorMessage, Exception e) {
        super(errorMessage, e);
    }
}
