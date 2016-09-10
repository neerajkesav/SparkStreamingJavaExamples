/**
 * Streaming
 */
package com.neeraj.streamingjava;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Class Streaming. Examples on Streaming.
 * 
 * @author neeraj
 *
 */
public class Streaming {
	/**
	 * Class Streaming contains three functions 'wordCount()', 'lineCount()' and
	 * 'windowCount()' to describe streaming. NetCat is used as Data Server by
	 * running nc -lk 9999.
	 */

	/**
	 * totalCount is used to count the total no. of lines streamed. lineCount is
	 * used to count the total no. of lines in a batch.
	 */
	public static long totalCount = 0;
	public static long linecount = 0;

	/**
	 * Counts the no. of words in each batch.
	 * 
	 * @param streamingContext
	 *            Takes 'JavaStreamingContext' from calling method.
	 * @param timeOut
	 *            Takes time out for streaming process in milliseconds.
	 */
	public static void wordCount(JavaStreamingContext streamingContext, long timeout) {

		/* Reading Streams */
		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 9999);

		/* Calculating word count. */
		JavaPairDStream<String, Integer> wordcount = lines.flatMap(x -> (Arrays.asList(x.split(" "))).iterator())
				.mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> x + y);

		/* Printing word count in current batch. */
		wordcount.print();
		wordcount.foreachRDD(x -> x.collect().forEach(y -> System.out.println(y)));

		/* Starting Streaming. */
		streamingContext.start();
		try {
			streamingContext.awaitTerminationOrTimeout(timeout); // waiting for
																	// termination.

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		/* Closing Streaming context. */
		streamingContext.close();

	}

	/**
	 * Counts the no. of lines in each batch and total no. of line streamed.
	 * 
	 * @param streamingContext
	 *            Takes 'JavaStreamingContext' from calling method.
	 */
	public static void lineCount(JavaStreamingContext streamingContext, long timeout) {

		/* Reading Streams */
		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 9999);

		/* Counting lineCount and totalCount. */
		@SuppressWarnings("serial")
		class ComputeLine implements Function<JavaRDD<String>, Void> {
			public Void call(JavaRDD<String> s) {
				linecount = s.count();
				totalCount += linecount;
				System.out.println(s.collect());
				System.out.println("Line count in RDD:" + linecount + "  Total Lines: " + totalCount);

				return null;
			}
		}

		/* Creating object of ComputeLine. */
		ComputeLine a = new ComputeLine();
		/* Counting for each JavaRDD. */
		lines.foreachRDD(x -> a.call(x));

		/* Starting Streaming. */
		streamingContext.start();
		try {
			streamingContext.awaitTerminationOrTimeout(timeout); // waiting for
																	// termination.

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		/* Closing Streaming context. */
		streamingContext.close();

	}

	/**
	 * window count in each batch.
	 * 
	 * @param streamingContext
	 *            Takes 'JavaStreamingContext' from calling method.
	 */
	public static void windowCount(JavaStreamingContext streamingContext, long timeout) {

		/* Reading Streams */
		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 9999);

		/* Count in the window */
		@SuppressWarnings("serial")
		class windowCount implements Function<JavaRDD<String>, Void> {

			public Void call(JavaRDD<String> s) {
				System.out.println("Window Rdd Count : " + s.count());
				return null;
			}
		}

		/* Creating windowRDD with window duration 4 and sliding duration 2. */
		JavaDStream<String> windowRdd = lines.window(Durations.seconds(4), Durations.seconds(2));

		/* Counting for each RDD. */
		windowRdd.foreachRDD(x -> new windowCount().call(x));

		/* Starting Streaming. */
		streamingContext.start();
		try {
			streamingContext.awaitTerminationOrTimeout(timeout); // waiting for
																	// termination.

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		/* Closing Streaming context. */
		streamingContext.close();

	}

}
