package com.saurabh.dada.iq.impl.iq.apigee;

public class Job implements Runnable{

	private final String jobName;
	public Job(String jobName){
		this.jobName=jobName;
	}
	
	@Override
	public void run() {
		try{
			Thread.sleep(250);
			System.out.println(this.jobName +" is currently executing!!");
			Thread.sleep(1000);
			System.out.println(this.jobName + " completed executing!!");
		}
		catch(InterruptedException e){
		  //do nothing
		}
	}
}
