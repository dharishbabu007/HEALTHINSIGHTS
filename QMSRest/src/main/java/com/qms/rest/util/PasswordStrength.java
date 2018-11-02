package com.qms.rest.util;

public class PasswordStrength {

	public static boolean metPasswordStrength(String password) {

		int digit = 0;
		int lowerCase = 0;
		int upperCase = 0;
		int count = 0;
		int len = password.length();
		char ch;

		if (len >= 8) {
			while (count < len) {
				ch = password.charAt(count);
				if (Character.isDigit(ch)) {
					digit = digit + 1;
				}
				if (Character.isLowerCase(ch)) {
					lowerCase = lowerCase + 1;
				}
				if (Character.isUpperCase(ch)) {
					upperCase = upperCase + 1;
				}
				count = count + 1;
			}
		}
		if (digit >= 1 && lowerCase >= 1 && upperCase >= 1)
			return true;

		return false;
	}

	
//	public static void main(String[] args) {
//		System.out.println(PasswordStrength.metPasswordStrength("TESTTa1"));
//	}
}
