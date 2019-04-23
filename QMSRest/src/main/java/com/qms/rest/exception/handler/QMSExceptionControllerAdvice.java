package com.qms.rest.exception.handler;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.qms.rest.exception.QMSException;
import com.qms.rest.model.RestResult;
  
//@RestControllerAdvice
public class QMSExceptionControllerAdvice extends ResponseEntityExceptionHandler {

    //@ExceptionHandler(QMSException.class)
    public final ResponseEntity<RestResult> handleProgramCreatorException(Exception ex, WebRequest request) {
		RestResult restResult = RestResult.getFailRestResult(ex.getMessage());
        return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
    }
}