package parallelwork;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * How to run: create the Jar using "mvn clean install" and run like below.
 * java -cp target/mytest-0.0.1-SNAPSHOT.jar parallelwork.WriteDataQ 50 2
 * 
 * args[0] : load : mandatory else exception
 * args[1] : number of workers thread :  default is 1
 * args[2] : 1 	: means default case that main thread will wait for producer to complete filling load.
 * 			 0	: means while producer is filling consumer can start working. 
 * @author Chandan
 */

public class WriteDataQ {
	
	public static final Logger log = LogManager.getLogger(WriteDataQ.class);
	
	LinkedBlockingQueue<String> q = new LinkedBlockingQueue<String>();
	
	int wc = 0; // workers count.
	
	final AtomicInteger loadCounter = new AtomicInteger(0);
	
	static int max_load = 10;
	
	public ExecutorService es = null;
	
	int waitForProducer = 1;
	
	ArrayList<CompletableFuture<Integer>> fl =  new ArrayList<CompletableFuture<Integer>>();
	
	public void init(String[] args) {
		max_load = Integer.parseInt(args[0]);
		wc = (null==args[1] || args[1].isEmpty() ) ? 1:Integer.parseInt(args[1]);
		
		if(args.length > 2)
			waitForProducer = (null==args[2] || args[2].isEmpty() ) ? 1:Integer.parseInt(args[1]);
		
		es = Executors.newFixedThreadPool(wc);
		
		int cpu = Runtime.getRuntime().availableProcessors();
		log.info("Initialization done: workerthreads=[{}] maxload=[{}] system cpu=[{}]", wc, max_load, cpu);
	}
	
	// to produce the load upto max_load.
	private void produceLoad() {
		
		Instant st0 = Instant.now();
		
		// Just load the tasks in q.
		CompletableFuture<Integer> producer = CompletableFuture.supplyAsync(() -> {
			try {
				for (int i = 0; i < max_load; i++) {

					q.put("LoadedString + " + i);

				}
			} catch (InterruptedException e) {		System.out.println("Error, may be Q is full");			}
			
			return max_load;
		}, es);
		
		Instant et0 = Instant.now();
		
		if(1==waitForProducer) {
			int x = producer.join();
			long t = Duration.between(st0, et0).toMillis();
			log.info("Producer has completed adding tasks.Total task=[{}], timetaken(ms)=[{}]", x,t);
		}
		
	}
	public void test() {
		
		produceLoad();
		
//		waitForUserInputToExit(); //after puzzle comment out this.
		
		log.info("Consumers are starting work. loadSize={}",q.size());
		Instant st = Instant.now();
		for(int i=0;i<wc;i++) fl.add( CompletableFuture.supplyAsync(()->doTask(),es));
		
		log.info("calling get, this will wait for all consumers to finish.");
		for (int i = 0; i < wc; i++) try {	fl.get(i).get();  } catch (Exception e) { e.printStackTrace();}
		Instant et = Instant.now();
		log.info("Consumers collectively finished all work. Total Work Time in main thread(ms)= *** [ {} ]ms *** ", Duration.between(st, et).toMillis());
		
	}
	
	public void doCleanup() {
		es.shutdown();
	}
	
	private int doTask() {
		Instant st = Instant.now();
		int r = 0;
		while( loadCounter.getAndIncrement()  < max_load ) {
			r++;
			try {
				String s = q.take();
				doCpuIntensiveTask(s);
			} catch (InterruptedException e) {	e.printStackTrace();	}
		}
		Instant et = Instant.now();
		log.info("The consumer completed in [{}]ms, load=[{}]", Duration.between(st, et).toMillis(),r);
		
		return r;
	}
	
	public void doWarmUpTask() {
		for (int i=0;i<wc;i++) fl.add( CompletableFuture.supplyAsync(()->{int k =0; for(;k<100;k++) {} return k;},es));
		for (int i = 0; i < wc; i++) try {	fl.get(i).get();  } catch (Exception e) { e.printStackTrace();}
		for (int i = wc - 1; i >=0; i--) try {	fl.remove(i);  } catch (Exception e) { e.printStackTrace();} // clean the list.
		log.info("warmup done. Now thread pool leaded with desired cosumers threads.");
		printThreadsPoolStats();
	}

	// hypothetical cpu intensive task.
	protected void doCpuIntensiveTask(String s) {
		byte[] ba = s.getBytes();
		ArrayList<String> list = new ArrayList<String>(ba.length);
		Random r = new Random(2);

		for (int i = 0; i < ba.length; i++) {
			list.add(r.nextInt() + "_" + ba[0]);
		}

		int count = 1000000;
		for (int i = 0; i < count; i++) {
			list.add(r.nextInt() + "ckjkjkjkjkkjkjkj");
		}

		for (int i = count; i > 0; i--) {
			list.remove(i);
		}
	}

	private void printThreadsPoolStats() {
		printThreadsPoolStats("");
	}
	
	private void printThreadsPoolStats(String msg) {
		ThreadPoolExecutor tp = (ThreadPoolExecutor) es;

		log.info("ExecutorsStats ( ActiveThreadCount=[{}] \t TaskCount=[{}] \t QueueSize=[{}] \t PoolSize=[{}] {} )",
				tp.getActiveCount(), tp.getTaskCount(), tp.getQueue().size(), tp.getPoolSize(),msg);
	}
	
	private void waitForUserInputToExit() {
		printThreadsPoolStats();
		Scanner s = new Scanner(System.in);
		log.info("Press any key and then enter...");
		String str = s.next();
	}
	
	public static void main(String[] args) {
		
		
		WriteDataQ w = new WriteDataQ();
		w.init(args);
		w.doWarmUpTask();
		w.test();
		
		
//		w.waitForUserInputToExit();
		w.doCleanup();
		
	}

}
