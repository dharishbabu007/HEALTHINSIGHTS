package com.qms.rest.util;

import java.security.SecureRandom;

public class PasswordGenerator {
	
	private static SecureRandom random = new SecureRandom();

    private static final String ALPHA_CAPS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String ALPHA = "abcdefghijklmnopqrstuvwxyz";
    private static final String NUMERIC = "0123456789";
    //private static final String SPECIAL_CHARS = "!@#$%^&*_=+-/";
    
    private static final int PASSWORD_LENGTH = 10;    
    //private static final String PASSWORD_DICTIONARIES = ALPHA_CAPS+NUMERIC+ALPHA+SPECIAL_CHARS;
    private static final String PASSWORD_DICTIONARIES = ALPHA_CAPS+NUMERIC+ALPHA;

    public static String generatePassword() {
	    String result = "";
	    for (int i = 0; i < PASSWORD_LENGTH; i++) {
	        int index = random.nextInt(PASSWORD_DICTIONARIES.length());
	        result += PASSWORD_DICTIONARIES.charAt(index);
	    }
	    return result;
    }

//    public static void main(String[] args) {
//	    String password = generatePassword();
//	    System.out.println(password);
//	    System.out.println();
//    }	

}
