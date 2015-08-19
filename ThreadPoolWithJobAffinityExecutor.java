package com.saurabh.dada.iq.impl.iq.apigee;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread pool implementation of ThreadPoolWithJobAffinity interface.
 * 
 * @author yadas
 * @version 1.0
 */

public class ThreadPoolWithJobAffinityExecutor implements
		ThreadPoolWithJobAffinity {
	
	private final ConcurrentHashMap<String, Worker> jobs;
	private volatile boolean shutdown;
	private final int poolSize;
	private final int jobQueueSize; 
	private final static int DEFAULT_JOB_QUEUE_SIZE=10;
	private final static int DEFAULT_THREAD_POOL_SIZE=10;
	private final static long DEFAULT_JOB_QUEUE_ADDITION_TIMEOUT=100; // 500 milliseconds
	private final Lock lock = new ReentrantLock();
	private final long jobQueueTimeout;
	
	/**
	 * Create a new ThreadPoolWithJobAffinity thread pool of queue length of default poolSize
	 *  DEFAULT_THREAD_POOL_SIZE = 10
	 *  DEFAULT_JOB_QUEUE_SIZE = 10
	 *  DEFAULT_JOB_QUEUE_ADDITION_TIMEOUT = 100 milliseconds
	 */
	public ThreadPoolWithJobAffinityExecutor(){
		this(DEFAULT_THREAD_POOL_SIZE,DEFAULT_JOB_QUEUE_SIZE, DEFAULT_JOB_QUEUE_ADDITION_TIMEOUT);
	}
	/**
	 * Create a new ThreadPoolWithJobAffinity thread pool of specified poolSize where every workerThread will have its own pool
	 * @param poolSize number of workerThreads ready for accepting the jobs
	 * @param jobQueueSize the core size of number of jobs that can be queued for the workerThread which executes the jobs of a given jobId
	 */
	public ThreadPoolWithJobAffinityExecutor(int poolSize, int jobQueueSize,long jobQueueTimeout){
		this.shutdown=false;
		this.poolSize=poolSize;
		this.jobQueueSize=jobQueueSize;
		this.jobQueueTimeout=jobQueueTimeout;
		this.jobs=new ConcurrentHashMap<>(poolSize);
	}

	@Override
	public int poolSize() {
		return this.poolSize;
	}
	
	public int totalRunningJobs(){
		return this.jobs.size();
	}

	/**
	 * Run the given Runnable job. If no worker thread is currently executing the jobs of given jobId, new worker thread is forked and started.
	 * If threadpool is full and no new workerThread can be added to the pool, jobs are queued and wait until an existing workerThread become free
	 * and then idle worker thread is removed and new worker thread for given jobId is added to the pool.
	 * 
	 * Runnable jobs are queue for given workerThread where queue size if defined by jobQueueSize. 
	 * Multiple queued jobs are executed in the order of their submission.
	 * 
	 * @param jobId - id of job
	 * @param job - Runnable job which has to be executed.
	 */
	@Override
	public void submit(String jobId, Runnable job) {
				
		if(!shutdown){
			Worker workerThread = jobs.get(jobId);
			if(workerThread==null){
				try{
					lock.lock();
					if(jobs.size()==this.poolSize){
						shutdownIdleWorkerThread();
					}
				}
				finally{
					lock.unlock();
				}
				workerThread = new Worker(jobId,UUID.randomUUID().toString(),this.jobQueueSize,this.jobQueueTimeout);
				workerThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
					public void uncaughtException(Thread myT, Throwable e) {
						System.out.println(myT.getName() + " throws exception: " + e);
					}
				});
				jobs.put(jobId, workerThread);
			}
			workerThread.addJobsToQueue(job);
		}
		else{
			//System.out.println("Couldn't accept new jobs, threadpool shutdown already initiated!");
			throw new IllegalStateException("Couldn't accept new jobs, threadpool shutdown already initiated!");
		}
	}
	
	private void shutdownIdleWorkerThread(){
		try{
			if(!shutdown){
				boolean removed=false;
				Worker mostIdle = null;
				int min=this.jobQueueSize+1;
				Set<Entry<String, Worker>> allWorkers = jobs.entrySet();
				while(!removed){
					for (Entry<String, Worker> entry : allWorkers) {
						Worker w = entry.getValue();
						if(w.getJobQueueSize()<min){
							mostIdle=w;
							min=w.getJobQueueSize();
						}
					}
					if(mostIdle!=null){
						removed=true;
						mostIdle.shutdownWorker();
						mostIdle.join();
						System.out.println("POOL_REMVOE_IDLE||WorkerThread: " + mostIdle.getCurrentWorkerThreadId() + " with jobId: " + mostIdle.wJobId + " is now removed!");
						jobs.remove(mostIdle.wJobId);
						break;
					}
				}
			}
		}
		catch(InterruptedException e){}
	}

	/**
	 * Shutdowns the current running Thread-pool and also awaits for the workerThreads to finish executing the queue completely before shutdown.
	 */
	@Override
	public void shutdown() {
		System.out.println("THREAD_POOL_SHUTDOWN|| Threadpool shutdown already initiated!");
		if(shutdown==true)
			//System.out.println("Threadpool shutdown already initiated!");
			throw new IllegalStateException("Threadpool shutdown already initiated!");
		else{
			joinAllWorkersBeforeShutdown();
		}
			
	}
	
	/**
	 * API to wait for the workerThreads to finish executing the remaining queue.
	 */
	private void joinAllWorkersBeforeShutdown(){
		try{
			Set<Entry<String, Worker>> allWorkers = jobs.entrySet();
			for (Entry<String, Worker> entry : allWorkers) {
				Worker w = entry.getValue();
				w.shutdownWorker();
				w.join();
				w.interrupt();
				
			}
		}
		catch(InterruptedException e){}
		finally{
			shutdown=true;
		}
	}
	
	/**
	 * Worker thread class which queue the jobs of same jobId and execute them in FIFO order. 
	 * It also wait for all the jobs to complete before the parent workerThread is terminated.
	 * 
	 * @author yadas
	 *
	 */
	private final class Worker extends Thread{
		
		private final LinkedBlockingQueue<Runnable> threadPool;
		private final String wJobId;
		private volatile boolean isActive=true;
		private volatile boolean drainQueue=false;
		private String tid;
		private final Condition isEmptyCondition;
		private final Condition shutdownCondition;
		private final Lock lock;
		private final long timeout;
		
		public Worker(String jobId,String tid,int poolSize, long timeout){
			this.wJobId=jobId;
			this.threadPool=new LinkedBlockingQueue<>(poolSize);
			this.tid=tid;
			this.lock=new ReentrantLock();
			this.isEmptyCondition = this.lock.newCondition();
			this.shutdownCondition = this.lock.newCondition();
			this.timeout=timeout;
			System.out.println("POOL_ADD_NEW|| No workerThread is currently executing jobs with id: " + jobId + ", adding " + this.getCurrentWorkerThreadId());
			start();
		}
		
		@Override
		public void run() {
			try{
				//System.out.println("WorkerThread: " + getCurrentWorkerThreadId() + " looking jobs with id: " + this.wJobId);
				while (isActive||drainQueue) {
					try {
						lock.lock();
						//System.out.println("WorkerThread: "	+ getCurrentWorkerThreadId() + " looping for jobs for jobId: " + this.wJobId);
						if(!isActive&&drainQueue&&this.threadPool.size()==0){
							drainQueue=false;
							shutdownCondition.signalAll();
						}
						if (this.threadPool.size()==0 && isActive && !drainQueue) {
							isEmptyCondition.await();
						}
						if (this.threadPool != null && this.threadPool.size()>0) {
							//System.out.println("J-Removing|| WorkerThread: " + getCurrentWorkerThreadId() + " on Job queue: " + this.wJobId);
							Runnable job = this.threadPool.poll();
							if(job!=null){
								Thread task = new Thread(job);
								task.start();
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						lock.unlock();
					}
					//System.out.println("WorkerThread: " + getCurrentWorkerThreadId() + " executing jobs with id: " + this.wJobId + " is shutting down!");
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		public void addJobsToQueue(Runnable job){
			if (isActive) {
				try {
					lock.lock();
					//System.out.println("J-Adding|| WorkerThread: " + getCurrentWorkerThreadId() + " gets new on Job queue: " + this.wJobId);
					this.threadPool.offer(job, timeout, TimeUnit.MILLISECONDS);
					isEmptyCondition.signalAll();
				} catch (InterruptedException e) {
				}
				finally{
					lock.unlock();
				}
			}
		}
		
		public void shutdownWorker(){
			this.drainQueue=true;
			this.isActive=false;
			try{
				lock.lock();
				isEmptyCondition.signalAll();
				shutdownCondition.await();
			}
			catch(InterruptedException e){}
			finally{
				lock.unlock();
				this.drainQueue=false;
			}
		}
		
		public String getCurrentWorkerThreadId(){
			return getName()+"::"+this.tid;
		}
		
		public int getJobQueueSize(){
			try{
				lock.lock();
				return this.threadPool.size();
			}
			finally{
				lock.unlock();
			}
		}
		
	}

}
