package com.qms.rest.exception.handler;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import com.qms.rest.exception.QMSException;
  
@ControllerAdvice
public class QMSExceptionControllerAdvice {
  
    @ExceptionHandler(QMSException.class)
    public ResponseEntity<String> exceptionHandler(Exception ex) {
        return new ResponseEntity<String>(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }
}