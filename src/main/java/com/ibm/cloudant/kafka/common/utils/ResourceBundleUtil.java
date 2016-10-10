package com.ibm.cloudant.kafka.common.utils;

import java.util.Locale;
import java.util.ResourceBundle;

public class ResourceBundleUtil {
	
	private static ResourceBundle rb = null;
	
	public static String get(String key){
		if(rb == null){
			Locale locale = new Locale("en", "US"); 
			rb = ResourceBundle.getBundle("message", locale);			
		}
		return rb.getString(key);
	}

}
