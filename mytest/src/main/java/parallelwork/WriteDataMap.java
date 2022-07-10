package parallelwork;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
 * C:\ckp\Soft\Eclipse\ws\mytest>C:\ckp\Soft\jdk-14.0.1\bin\java -cp target/mytest-0.0.1-SNAPSHOT.jar parallelwork.WriteDataMap 10 1 5
 * 
 * args[0] : load : mandatory else exception
 * args[1] : number of workers thread :  default is 1
 * args[2] : the bucket size; the toal load (specified by args[0) is distributed in buckets
 * 			 and each bucket size is defined by this CHUNK size. default is 1.
 * args[3] : 1 	: means default case that main thread will wait for producer to complete filling load.
 * 			 0	: means while producer is filling consumer can start working. 
 * @author Chandan
 */

public class WriteDataMap {
	
	public static final Logger log = LogManager.getLogger(WriteDataMap.class);
	
	static volatile ConcurrentHashMap<Integer, List<String>> loadMap = new ConcurrentHashMap<Integer, List<String>>();
	
	int wc = 0; // workers count.
	
	final AtomicInteger loadCounter = new AtomicInteger(0);
	
	static int max_load = 10;
	
	int bucket = 0;
	
	public ExecutorService es = null;
	
	int waitForProducer = 1;
	
	ArrayList<CompletableFuture<Integer>> fl =  new ArrayList<CompletableFuture<Integer>>();
	
	static int CHUNK = 10;
	
	public void init(String[] args) {
		max_load = Integer.parseInt(args[0]);
		wc = (null==args[1] || args[1].isEmpty() ) ? 1:Integer.parseInt(args[1]);
		
		if(args.length > 2)
			CHUNK = (null==args[2] || args[2].isEmpty() ) ? 1:Integer.parseInt(args[2]);
		
		if(args.length > 3)
			waitForProducer = (null==args[3] || args[3].isEmpty() ) ? 1:Integer.parseInt(args[3]);
		
		es = Executors.newFixedThreadPool(wc);
		
		int cpu = Runtime.getRuntime().availableProcessors();
		log.info("Initialization done: workerthreads=[{}] maxload=[{}] chunk=[{}] system cpu=[{}]", wc, max_load, CHUNK, cpu);
	}
	
	// to produce the load upto max_load.
	private void produceLoad() {
		
		Instant st0 = Instant.now();
		
		// Just load the tasks in q.
		CompletableFuture<Integer> producer = CompletableFuture.supplyAsync(() -> {
				
				List<String> locallist = new ArrayList<String>(CHUNK*2);
				
				int j = 0;
				for (int i = 0 ; i < max_load; i++) {
					
					if(j++< CHUNK)  locallist.add("LoadedString" + i);  
					
					if(j== CHUNK) {
						loadMap.put(bucket++, locallist);
						j = 0;	locallist = new ArrayList<String>(CHUNK*2); //reset
					}
				}
				
				if(j>0) loadMap.put(bucket++, locallist); //Left over in last loop of above code.
			
			return max_load;
			
		}, es);
		
		Instant et0 = Instant.now();
		
		if(1==waitForProducer) {
			int x = producer.join(); long t = Duration.between(st0, et0).toMillis();
			log.info("Producer has completed adding tasks.Total task=[{}], timetaken(ms)=[{}] loadMapSize={}", x,t,loadMap.size());
		}
	}
	
	public void test() {

		produceLoad();
		
		log.info("Consumers are starting work. loadMapSize={}",loadMap.size());
		Instant st = Instant.now();
		for(int i=0;i<wc;i++){ fl.add( CompletableFuture.supplyAsync(()->doTask(),es));}
		
		log.info("calling get, this will wait for all consumers to finish.");
		for (int i = 0; i < wc; i++) try {	fl.get(i).get();  } catch (Exception e) { e.printStackTrace();}
		
		Instant et = Instant.now(); 
		log.info("Consumers collectively finished all work. Total Work Time in main thread(ms)=[{}]ms",	Duration.between(st, et).toMillis());
		
	}
	
	public void doCleanup() {
		es.shutdown();
	}
	
	private int doTask() {
		Instant st = Instant.now();
		int r = 0;
		int k = 0;
		int c = 0;
		while ((k = loadCounter.getAndIncrement()) < bucket) {
			c++;
			List<String> load = loadMap.get(k);
			r += load.size();
//			log.info("Consumer getting chunk from map with key=[{}] loadSize=[{}]", k, load.size());
			for (String s : load) {
				doCpuIntensiveTask(s);
			}
		}
		Instant et = Instant.now();
		log.info("The consumer completed in [{}]ms, load=[{}], looped count=[{}] ( to get chunk from map )", Duration.between(st, et).toMillis(),r,c);
		return r;
	}
	
	// hypothetical cpu intensive task.
	private void doCpuIntensiveTask(String s) {
		Instant st =  Instant.now();
		byte[] ba = s.getBytes();
		ArrayList<String> list = new ArrayList<String>(ba.length);
		Random r = new Random(2);
		
		for(int i=0;i<ba.length;i++) {
			list.add(r.nextInt() + "_"+ba[0]);
		}
		
		int count = 1000000;
		for(int i=0;i<count;i++) {
			r = new Random(i);
			list.add(r.nextInt()+ "ckjkjkjkjkkjkjkj");
		}
		
		for(int i=count;i>0;i--) {
			list.remove(i);
		}
		
		Instant et = Instant.now();
//		log.info("cpu intensive execution time(ms) {}", Duration.between(st, et).toMillis());
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
		
		
		WriteDataMap w = new WriteDataMap();
		w.init(args);
		w.test();
		
		
//		w.waitForUserInputToExit();
		w.doCleanup();
		
	}

}
