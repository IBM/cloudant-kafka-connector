package com.ibm.cloudant.kafka.common.utils;

import org.apache.log4j.Logger;


public class UnitOfWork {
	private static Logger LOG = Logger.getLogger(UnitOfWork.class);

	private int totalNumberOfOperations;
	private int numberOfOperationsCompleted;
	private final long unitOfWorkNumber;

	// Stores the sequence number of the last operation.
	private String lastOperationSequenceNumber = null;

	public UnitOfWork(long unitOfWorkNumber, int totalNumberOfOperations) {
		this.unitOfWorkNumber = unitOfWorkNumber;
		this.totalNumberOfOperations = totalNumberOfOperations;
	}

	/**
	 * Determine if this unit of work is complete. The unit of work is
	 * considered complete if numberOfOperationsLeft is equal to zero.
	 * 
	 * @return
	 */
	public synchronized boolean isComplete() {
		if (numberOfOperationsCompleted > totalNumberOfOperations){
			LOG.warn(String.format("UOW %d: totalNumberOfOperations (%d) is less than numberOfOperationsCompleted (%d)", unitOfWorkNumber, totalNumberOfOperations, numberOfOperationsCompleted));
		}
		return numberOfOperationsCompleted >= totalNumberOfOperations;
	}

	/**
	 * Increase the number of operations by operations.
	 * @param operations TODO
	 * @param num
	 */
	public synchronized void increNumberOfOperations(int operations) {
		numberOfOperationsCompleted += operations;
		
		/*if (numberOfOperationsCompleted < totalNumberOfOperations) {
			numberOfOperationsCompleted += operations;
			return;
		}

		LOG.error("numberOfOperationsCompleted = " + numberOfOperationsCompleted + 
				", totalNumberOfOperations = " + totalNumberOfOperations + 
				", operations = " + operations);
		throw new AssertionError(
				"The number of operations left should not be decrement lower than zero. numberOfOperationsLeft:"
						+ numberOfOperationsCompleted);*/
	}

	/**
	 * Increase the number of total operations by totalOperations.
	 * @param operations TODO
	 * @param num
	 */
	synchronized protected void increNumberOfTotalOperations(int totalOperations) {
		totalNumberOfOperations += totalOperations;
	}

	
	/**
	 * Get the sequence number of the last operation.
	 * 
	 * @return
	 */
	public String getLastOperationSequenceNumber() {
		return lastOperationSequenceNumber;
	}
	
	protected void setLastOperationSequenceNumber(
			String lastOperationSequenceNumber) {
		this.lastOperationSequenceNumber = lastOperationSequenceNumber;
	}
	
	public long getUnitOfWorkNumber() {
		return unitOfWorkNumber;
	}
	
	/**
	 * Change the total number of operations for a unit of work.  This should only 
	 * be done if we want the unit of work to finish sooner than intended.
	 * 
	 * @param totalNumberOfOperations
	 */
	synchronized public void setTotalNumberOfOperations(int totalNumberOfOperations) {
		this.totalNumberOfOperations = totalNumberOfOperations;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append(" unitOfWorkNumber").append(unitOfWorkNumber)
		.append(" totalNumberOfOperations:").append(totalNumberOfOperations)
		.append(" numberOfOperationsCompleted:").append(numberOfOperationsCompleted)
		.append(" lastOperationSequenceNumber:").append(lastOperationSequenceNumber);
		
		return sb.toString();
	}
}
