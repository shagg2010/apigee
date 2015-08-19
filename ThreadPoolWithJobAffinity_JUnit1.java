package com.saurabh.dada.iq.impl.iq.apigee;

import static org.junit.Assert.*;

import org.junit.Test;

public class ThreadPoolWithJobAffinity_JUnit1 {

	@Test
	public void testThreadPool(){
		ThreadPoolWithJobAffinity threadPoolWithJobAffinity1 = new ThreadPoolWithJobAffinityExecutor(5,5);
		for(int i=0;i<100;i++){
			String jobId = new String("jid"+((i%10)+1)+"-1");
			Job job = new Job("t.1-"+(i+1));
			threadPoolWithJobAffinity1.submit(jobId, job);
		}
		threadPoolWithJobAffinity1.shutdown();
		assertFalse(((ThreadPoolWithJobAffinityExecutor)threadPoolWithJobAffinity1).isRunning());
	}

}
