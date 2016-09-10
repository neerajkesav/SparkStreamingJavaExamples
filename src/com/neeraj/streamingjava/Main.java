/**
 * Main
 */
package com.neeraj.streamingjava;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Main Class. To test and run Spark Streaming Java Examples.
 * 
 * @author neeraj
 *
 */
public class Main {
	/**
	 * Main Class contains main method to run Spark Streaming Java Examples. 
	 * - Word Count 
	 * - Line Count 
	 * - Window Count 
	 * 
	 */

	/**
	 * Runs Spark Streaming Java Examples.
	 * 
	 * @param args
	 *            Takes nothing.
	 */
	public static void main(String[] args) {

		/*
		 * Creating SparkContext. StreamingContext is created within the
		 * SparkContext. Runs on 2 threads, one for streaming and other for
		 * processing. Setting batch duration as 2 seconds.
		 */
		CreateSpark spark = new CreateSpark();
		JavaSparkContext sparkContext = spark.context("Spark Streaming Java Examples", "local[2]");
		JavaStreamingContext streamContext = spark.streaming(sparkContext, 2);

		/* Word Count. */
		Streaming.wordCount(streamContext, 60000);

		/* Line Count. */
		Streaming.lineCount(streamContext, 60000);		

		/* Window Count. */
		Streaming.windowCount(streamContext, 60000);

	}

}
