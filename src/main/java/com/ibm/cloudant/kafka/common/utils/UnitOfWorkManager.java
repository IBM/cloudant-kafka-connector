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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

/**
 * Keeps track of unit of works for the operations. To make sure we efficiently
 * write sequence numbers, we group the documents into "Unit Of Works". Only
 * when all the documents in the same unit of work is written to the target can
 * we update the sequence number in the storage.
 *
 */
public class UnitOfWorkManager {
	private static Logger LOG = Logger.getLogger(UnitOfWorkManager.class);
	

	private LinkedHashMap<Long, UnitOfWork> unitOfWorks;

	// stores the current unit of work number. This is used as the key to insert
	// uow to our LinkedHashMap(unitOfWorks)
	// Every time a new unit of work is created, this number increments.
	private long currentUnitOfWorkNumber;

	public UnitOfWorkManager() {
		unitOfWorks = new LinkedHashMap<Long, UnitOfWork>();
		currentUnitOfWorkNumber = 0;
	}

	/**
	 * Add a new UnitOfWork to our LinkedHashMap. currentUnitOfWorkNumber is
	 * incremented by 1.
	 * 
	 * @param numberOfOperations
	 * @param lastOperationSequenceNumber
	 */
	synchronized public void addUnitOfWork(int numberOfOperations) {
		UnitOfWork uow = new UnitOfWork(++currentUnitOfWorkNumber, numberOfOperations);
		unitOfWorks.put(currentUnitOfWorkNumber, uow);
	}

	/**
	 * Get the most recent current unit of work number.
	 * 
	 * @return
	 */
	synchronized public long getCurrentUnitOfWorkNumber() {
		return currentUnitOfWorkNumber;
	}

	/**
	 * Increment the number of operations completed for a particular unit of work.
	 * 
	 * @param uowNumber
	 *            the key to which the unit of work is stored inside our
	 *            LinkedHashMap
	 */
	synchronized public void incrNumberOfOperations(long uowNumber, int operations) {
		UnitOfWork uow = unitOfWorks.get(uowNumber);
		if (uow != null) {
			uow.increNumberOfOperations(operations);
		}
	}

	/**
	 * Increment the number of operations completed for a particular unit of work.
	 * 
	 * @param uowNumber
	 *            the key to which the unit of work is stored inside our
	 *            LinkedHashMap
	 */
	synchronized public void incrNumberOfTotalOperations(long uowNumber, int operations) {
		UnitOfWork uow = unitOfWorks.get(uowNumber);
		if (uow != null){
			uow.increNumberOfTotalOperations(operations);
		}
	}

	
	/**
	 * Get the oldest completed unit of work and remove it from the LinkedHashMap(unitOfWorks)
	 *
	 * @return null if no unit of work is completed
	 */
	synchronized public UnitOfWork getOldestCompletedUnitOfWork()
	{
		Iterator<Entry<Long, UnitOfWork>> iter = unitOfWorks.entrySet()
				.iterator();
		if (iter.hasNext()) {
			UnitOfWork uow = iter.next().getValue();
			if (uow.isComplete())
			{
				iter.remove();
				LOG.debug("getOldestCompletedUnitOfWork: " + uow );
				return uow;
			}
		}

		return null;
	}
	
	/**
	 * Go through the list of unit of works and set the lastOperationSequenceNumber for
	 * the first unit of work that does not have a lastOperationSequenceNumber
	 * 
	 * @param lastOperationSequenceNumber
	 */
	synchronized public void setLastOperationSequenceNumber(String lastOperationSequenceNumber)
	{
		Iterator<Entry<Long, UnitOfWork>> iter = unitOfWorks.entrySet()
				.iterator();
		while (iter.hasNext()) {
			UnitOfWork uow = iter.next().getValue();
			if (uow.getLastOperationSequenceNumber() == null)
			{
				uow.setLastOperationSequenceNumber(lastOperationSequenceNumber);
				LOG.debug("setLastOperationSequenceNumber: " + uow );
				break;
			}
		}
	}

	/**
	 * Get the number of unit of works in the LinkedHashMap.
	 * 
	 * @return
	 */
	synchronized public int getNumberOfUnitOfWorks() {
		return unitOfWorks.size();
	}
	
	/**
	 * Set the information for the last unit of work including altering its total number of operations
	 * and the lastOperationSequenceNumber.
	 * 
	 * The idea is that we may not want to wait for a long time for all the operations in the last unit of work to finish.
	 * By changing the total number of operations(to a lower number), the last unit of work can complete earlier.
	 * 
	 * @param totalNumber
	 * @param lastOperationSequenceNumber
	 */
	synchronized public void setLastUnitOfWorkInfo(int totalNumber, String lastOperationSequenceNumber)
	{
		Iterator<Entry<Long, UnitOfWork>> iter = unitOfWorks.entrySet()
				.iterator();
		UnitOfWork uow = null;
		while (iter.hasNext()) {
			uow = iter.next().getValue();
		}
		
		if (uow != null)
		{
			uow.setLastOperationSequenceNumber(lastOperationSequenceNumber);
			uow.setTotalNumberOfOperations(totalNumber);
			LOG.debug("last unitofwork: " + uow);
		}
		
	}
	
	synchronized public String toString()
	{
		Iterator<Entry<Long, UnitOfWork>> iter = unitOfWorks.entrySet()
				.iterator();
		UnitOfWork uow = null;
		String ret = new String();
		while (iter.hasNext()) {
			uow = iter.next().getValue();
			ret += uow.toString();
		}

		return ret;
	}


}
