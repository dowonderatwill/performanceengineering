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
 * C:\ckp\Soft\Eclipse\ws\mytest>C:\ckp\Soft\jdk-14.0.1\bin\java -cp target/mytest-0.0.1-SNAPSHOT.jar parallelwork.WriteDataQ 50 2
 * 
 * args[0] : load : mandatory else exception
 * args[1] : number of workers thread :  default is 1
 * args[2] : 1 	: means default case that main thread will wait for producer to complete filling load.
 * 			 0	: means while producer is filling consumer can start working. 
 * @author Chandan
 */

public class WriteDataSimple {
	
	public static final Logger log = LogManager.getLogger(WriteDataSimple.class);
	
	int wc = 0; // workers count.
	
	static int max_load = 10;
	
	public ExecutorService es = null;
	
	final static ArrayList<CompletableFuture<Integer>> fl =  new ArrayList<CompletableFuture<Integer>>();
	
	public void init(String[] args) {
		max_load = Integer.parseInt(args[0]);
		wc = (null==args[1] || args[1].isEmpty() ) ? 1:Integer.parseInt(args[1]);
		es = Executors.newFixedThreadPool(wc);
		int cpu = Runtime.getRuntime().availableProcessors();
		log.info("Initialization done: workerthreads=[{}] maxload=[{}] system cpu=[{}]", wc, max_load, cpu);
	}
	
	public void test() {
		
		log.info("--- Consumers are starting work. ---");
		Instant st = Instant.now();
		final int load = max_load; 
		for(int i=0;i<wc;i++) fl.add( CompletableFuture.supplyAsync(()->doTask(load, true),es));
		
		log.info("calling get, this will wait for all consumers to finish.");
//		for (int i = 0; i < wc; i++) try {	log.info("consumer [{}] \t completed in [{}]ms", i, fl.get(i).get());  } catch (Exception e) { e.printStackTrace();}
		for (int i = 0; i < wc; i++) try {	fl.get(i).get();  } catch (Exception e) { e.printStackTrace();}
		Instant et = Instant.now();
		log.info("*** Consumers collectively finished all work. Total Work Time in main thread(ms)=[{}]ms ***", Duration.between(st, et).toMillis());
		
	}
	
	public void doWarmUpTask() {
		for(int i=0;i<wc;i++) fl.add( CompletableFuture.supplyAsync(()->doTask(1,false),es));
		for (int i = 0; i < wc; i++) try {	fl.get(i).get();  } catch (Exception e) { e.printStackTrace();}
		for (int i = wc - 1; i >=0; i--) try {	fl.remove(i);  } catch (Exception e) { e.printStackTrace();} // clean the list.
		log.info("warmup done. Now thread pool leaded with desired cosumers threads.");
		printThreadsPoolStats();
	}
	
	public void doCleanup() {
		es.shutdown();
	}
	
	private int doTask(int load, boolean printLog) {
		Instant st = Instant.now();
		int r = 0;
		
		String s = "someSampleStrings";
		ArrayList<String> list = new ArrayList<String>(s.length());
		Random rad = new Random(29);
		while( r++  < load ) {
			doCpuIntensiveTask(s,rad,list);
		}
		Instant et = Instant.now();
		
		long retTime = Duration.between(st, et).toMillis();
		if(true == printLog)	log.info("The consumer completed in [{}]ms, load=[{}]", retTime,--r);
		return (int)retTime;
	}
	
	// hypothetical cpu intensive task.
	private void doCpuIntensiveTask(String s, Random r, ArrayList<String> list ) {
		byte[] ba = s.getBytes();
		list = new ArrayList<String>(ba.length);
		
		for(int i=0;i<ba.length;i++) {
			list.add(r.nextInt() + "_"+ba[0]);
		}
		
		int count = 1000000;
		for(int i=0;i<count;i++) {
			list.add(r.nextInt()+ "ckjkjkjkjkkjkjkj");
		}
		
		for(int i=count;i>0;i--) {
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
		log.info("Press any key and then enter to exit...");
		String str = s.next();
		log.info("Exiting..");
		
	}
	
	public static void main(String[] args) {
		
		
		WriteDataSimple w = new WriteDataSimple();
		w.init(args);
		w.doWarmUpTask();
		w.test();
		
//		w.waitForUserInputToExit();
		w.doCleanup();
		
	}

}