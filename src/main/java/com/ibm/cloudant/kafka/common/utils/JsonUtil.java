package com.ibm.cloudant.kafka.common.utils;

import org.json.JSONObject;

public class JsonUtil {
	
	/**
	 * Get a string value from a JSON document. Returns null if the value doesn't exist in the document.
	 * 
	 * @param json
	 * @param attributeName
	 * @return
	 */
	public static String getStringValue(JSONObject json, String attributeName)
	{
		Object attribute = json.get(attributeName);
		
		if (attribute != null)
		{
			return (String)attribute;
		}
		
		return null;
	}
}
