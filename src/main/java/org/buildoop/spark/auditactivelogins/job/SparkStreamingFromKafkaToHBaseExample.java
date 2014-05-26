package org.buildoop.spark.auditactivelogins.job;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.buildoop.spark.auditactivelogins.hbase.HBaseCounterIncrementor;

import com.google.common.collect.Lists;

import scala.Tuple2;

//import com.cloudera.sa.sparkonalog.hbase.HBaseCounterIncrementor;
import com.google.common.base.Optional;

public class SparkStreamingFromKafkaToHBaseExample {
	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err
					.println("Usage: SparkStreamingFromFlumeToHBaseExample {master} {zkQuorum} {group} {topic} {numThreads} {table} {columnFamily}");
			System.exit(1);
		}

		String master = args[0];
		String zkQuorum = args[1];
		String group = args[2];
		String[] topics = args[3].split(",");
		int numThreads = Integer.parseInt(args[4]);
		String tableName = args[5];
		String columnFamily = args[6];
		
		
		Duration batchInterval = new Duration(2000);

		JavaStreamingContext sc = new JavaStreamingContext(master,
				"KafkaEventCount", batchInterval,
				System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(SparkStreamingFromKafkaToHBaseExample.class));
		
	   
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    
	    for (String topic: topics) {
	      topicMap.put(topic, numThreads);
	    }
		
		final Broadcast<String> broadcastTableName = sc.sparkContext().broadcast(tableName);
		final Broadcast<String> broadcastColumnFamily = sc.sparkContext().broadcast(columnFamily);
		
		//JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream(host, port);
		
		
		JavaPairDStream<String, String> messages = KafkaUtils.createStream(sc, args[1], args[2], topicMap);

	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      @Override
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });
	    
	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction <String, String>(){
	        @Override
	        public Iterable<String> call(String x) {
	          return Lists.newArrayList(SPACE.split(x));
	        }
	    });
	    
	    JavaPairDStream<String, Integer> wordCounts = words.map(
	    	      new PairFunction<String, String, Integer>() {
	    	        @Override
	    	        public Tuple2<String, Integer> call(String s) {
	    	          return new Tuple2<String, Integer>(s, 1);
	    	        }
	    	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	    	        @Override
	    	        public Integer call(Integer i1, Integer i2) {
	    	          return i1 + i2;
	    	        }
	    	      });
	    
		wordCounts.foreach(new Function2<JavaPairRDD<String,Integer>, Time, Void>() {

			@Override
			public Void call(JavaPairRDD<String, Integer> values,
					Time time) throws Exception {
				
				values.foreach(new VoidFunction<Tuple2<String, Integer>> () {

					@Override
					public void call(Tuple2<String, Integer> tuple)
							throws Exception {
						HBaseCounterIncrementor incrementor = 
								HBaseCounterIncrementor.getInstance(broadcastTableName.value(), broadcastColumnFamily.value());
						incrementor.increment("Counter", tuple._1(), tuple._2());
						System.out.println("------------------------------- Counter:" + tuple._1() + "," + tuple._2());
						
					}} );
				
				return null;
			}});

		sc.start();


		
		//JavaDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(sc, host, port);
		/*
		JavaPairDStream<String, Integer> lastCounts = flumeStream
				.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {

					@Override
					public Iterable<String> call(SparkFlumeEvent event)
							throws Exception {
						String bodyString = new String(event.event().getBody()
								.array(), "UTF-8");
						return Arrays.asList(bodyString.split(" "));
					}
				}).map(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String str)
							throws Exception {
						return new Tuple2(str, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer x, Integer y) throws Exception {
						// TODO Auto-generated method stub
						return x.intValue() + y.intValue();
					}
				});
				
				
				lastCounts.foreach(new Function2<JavaPairRDD<String,Integer>, Time, Void>() {

					@Override
					public Void call(JavaPairRDD<String, Integer> values,
							Time time) throws Exception {
						
						values.foreach(new VoidFunction<Tuple2<String, Integer>> () {

							@Override
							public void call(Tuple2<String, Integer> tuple)
									throws Exception {
								HBaseCounterIncrementor incrementor = 
										HBaseCounterIncrementor.getInstance(broadcastTableName.value(), broadcastColumnFamily.value());
								incrementor.incerment("Counter", tuple._1(), tuple._2());
								System.out.println("Counter:" + tuple._1() + "," + tuple._2());
								
							}} );
						
						return null;
					}});
		
		sc.start();
		*/
	}
}
