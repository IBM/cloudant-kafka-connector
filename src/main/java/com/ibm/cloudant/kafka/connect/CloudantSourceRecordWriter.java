package com.ibm.cloudant.kafka.connect;

public class CloudantSourceRecordWriter implements Runnable{
	public static volatile int counter = 0;
		
	public CloudantSourceRecordWriter() {
	}
	
	public void run() {
		counter++;
        System.out.println(Thread.currentThread().getName() + " => Counter: " + counter);                      
	}
}
