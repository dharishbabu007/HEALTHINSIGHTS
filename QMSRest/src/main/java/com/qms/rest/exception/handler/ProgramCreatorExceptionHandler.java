package com.qms.rest.exception.handler;

import com.qms.rest.exception.ProgramCreatorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class ProgramCreatorExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ProgramCreatorException.class)
    public final ResponseEntity<Object> handleProgramCreatorException(Exception ex, WebRequest request) {
        return new ResponseEntity<Object>(ex, HttpStatus.BAD_REQUEST);
    }
}
