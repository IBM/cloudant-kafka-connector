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

import java.util.List;

import org.apache.log4j.Logger;

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ChangesResult;
import com.ibm.cloudant.kafka.common.CloudantConst;
import com.ibm.cloudant.kafka.common.MessageKey;

import com.ibm.cloudant.kafka.common.utils.ResourceBundleUtil;

import org.json.*;

public class JavaCloudantOneTimeFeed {

	static private Logger log = Logger.getLogger(JavaCloudantOneTimeFeed.class);
	
	private		String		lastSeq_		=	"0";
	private		boolean		closeRequested_	=	false;
	private		boolean		isClosed_		=	false;
	private		String		closeMessage_	=	null;
	private		JSONObject	jsonToReturn_	=	null;
	private		boolean		hasNextCalled_	=	false;
	private		boolean		allDocsRead_	=	false;
	
	private     CloudantClient cantClient = null;
	private     ChangesResult cantChangeResult = null;
	private     Database cantDB = null; 
	private     String cantDBName = null;

	private     List<ChangesResult.Row> cantRows = null;
	private     int rowIndex = 0;
	private     long totalRows = 0;

	
	public JavaCloudantOneTimeFeed(String canturl, String cantuser, String cantPwd, String lastSeq) {
		init(canturl, cantuser, cantPwd, lastSeq, false);
	}
	
	public JavaCloudantOneTimeFeed(String canturl, String cantuser, String cantPwd, String lastSeq, boolean includeDocs) {
		init(canturl, cantuser, cantPwd, lastSeq, includeDocs);
	}
	
	private void init(String canturl, String cantuser, String cantPwd, String lastSeq, boolean includeDocs)  {

		if(lastSeq != null) {
			this.lastSeq_	=	lastSeq;
		} else {
			this.lastSeq_ = "0";
		}

		try {
			
			log.debug(">JavaCloudantOneTimeFeed for " + canturl);
								
			// Create a new CloudantClient instance for account endpoint account.cloudant.com
			String urlWithoutProtocal = canturl.substring(canturl.indexOf("://") +3);
			String account = urlWithoutProtocal.substring(0,urlWithoutProtocal.indexOf("."));
			
			cantClient = ClientBuilder.account(account)
			                          .username(cantuser)
			                          .password(cantPwd)
			                          .build();
			// Create a database instance
			cantDBName = canturl.substring(canturl.lastIndexOf("/")+1);
			
			// Create a database instance
			cantDB = cantClient.database(cantDBName, false);
			log.debug("since" + lastSeq_);
			cantChangeResult = cantDB.changes()
		                        .includeDocs(true)
		                        .since(lastSeq_)
		                        .limit(CloudantConst.NUMBER_OF_DOCS_IN_ONETIME_FEED)
		                        .getChanges();
		
			cantRows = cantChangeResult.getResults();
			totalRows = cantRows.size();
			
			log.debug(" the number of row read from feed: " + totalRows);
			
			log.debug("<JavaCloudantOneTimeFeed for " + canturl);
		} catch(Exception e) {
			throw new RuntimeException(String.format(ResourceBundleUtil.get(
					MessageKey.READ_CLOUDANT_STREAM_ERROR), e.getMessage()));
		}
	}
	
	
	public void stopFeed() {
		if(isClosed_) {
			/*The Feed is already closed*/
			return;
		}
		
		closeRequested_	= true;
		if(allDocsRead_) {
			closeFeed(this.closeMessage_);
		}
	}

	public String getStopMessage() {
		return closeMessage_;
	}


	public JSONObject getNextChange() {
		if(isClosed_) {
			throw new RuntimeException(ResourceBundleUtil.get(MessageKey.STREAM_CLOSED_ERROR));
		}
		
		if( !hasNextCalled_ ) {
			getNextLineAndBuildJSONDoc();
		}
		hasNextCalled_ = false;
		JSONObject objToReturn = jsonToReturn_;
		
		/*Set json to return to null, so that same value is not read next time in case of failure and no update*/
		jsonToReturn_ = null;
		
		return objToReturn;
	}

	public boolean isStopped() {
		return isClosed_;
	}

	private void closeFeed(String message) {
		try {
			cantClient.shutdown();
		} catch (Exception e) {
			throw new RuntimeException(String.format(ResourceBundleUtil.get(
					MessageKey.STREAM_TERMINATE_ERROR), e.getMessage()));
		}
		isClosed_ = true;
		closeMessage_ = message;
	}
	
	private	boolean getNextLineAndBuildJSONDoc() {
		try {

			if((rowIndex)==totalRows) {
				// read new docs
				log.debug("since" + lastSeq_ + " limit" + CloudantConst.NUMBER_OF_DOCS_IN_ONETIME_FEED);
				cantChangeResult = cantDB.changes()
			                        .includeDocs(true)
			                        .since(lastSeq_)
			                        .limit(CloudantConst.NUMBER_OF_DOCS_IN_ONETIME_FEED)
			                        .getChanges();				

				cantRows = cantChangeResult.getResults();
				totalRows = cantRows.size();
				rowIndex = 0;
				log.debug("read new docs, count:" + totalRows);

				// if no more docs can be read
				if (totalRows == 0) {
					allDocsRead_ = true;
					closeFeed("All docs read");
					return false;
				}
			}
			
			String doc = cantRows.get(rowIndex).getDoc().toString();
			String seq = cantRows.get(rowIndex).getSeq();
			String id = cantRows.get(rowIndex).getId();
			boolean isDeleted = cantRows.get(rowIndex).isDeleted();
		    		
			JSONTokener tokener = new JSONTokener(doc);		
			jsonToReturn_ = new JSONObject();
			jsonToReturn_.put(CloudantConst.CLOUDANT_DOC, new JSONObject(tokener));
			
			jsonToReturn_.put(CloudantConst.CLOUDANT_DOC_ID, id);
			jsonToReturn_.put(CloudantConst.SEQ, seq);
			jsonToReturn_.put(CloudantConst.DELETED, isDeleted);
			
			rowIndex++;
			lastSeq_ = seq;
			
			
		} catch (Exception e) {
			throw new RuntimeException(String.format(ResourceBundleUtil.get(
					MessageKey.READ_CLOUDANT_STREAM_ERROR), e.getMessage()));
		}

		if(closeRequested_) {
			closeFeed("Stopped as per user request");
			return false;
		}
		return true;
	}
	
	public boolean hasMoreChanges() {
		boolean hasDoc = false;
		if(isClosed_) {
			throw new RuntimeException(ResourceBundleUtil.get(
					MessageKey.STREAM_CLOSED_ERROR));
		}
		
		if(allDocsRead_) {
			return false;
		}
		
		hasDoc = getNextLineAndBuildJSONDoc();
		hasNextCalled_ = true;
		return hasDoc;
	}
}