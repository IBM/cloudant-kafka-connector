/*******************************************************************************
* Copyright (c) 2016 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/
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
