package com.qms.rest.exception.handler;

import com.qms.rest.exception.ProgramCreatorException;
import com.qms.rest.model.RestResult;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class ProgramCreatorExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ProgramCreatorException.class)
    public final ResponseEntity<RestResult> handleProgramCreatorException(Exception ex, WebRequest request) {
//		HttpHeaders headers = new HttpHeaders();
//		headers.setContentType(MediaType.TEXT_PLAIN);
		RestResult restResult = RestResult.getFailRestResult(ex.getMessage());
        return new ResponseEntity<RestResult>(restResult, HttpStatus.BAD_REQUEST);
    }
}
