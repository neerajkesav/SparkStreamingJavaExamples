/**
 * CreateSpark
 */
package com.neeraj.streamingjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Class CreateSpark. Creates 'JavaSparkContext' and 'JavaStreamingContext'.
 * 
 * @author neeraj
 *
 */
public class CreateSpark {
	/**
	 * Class CreateSpark implements two functions 'context()' and 'streaming()' to
	 * create JavaSparkContext and JavaStreamingContext.
	 */

	/**
	 * Function 'context()' creates JavaSparkContext using the specified Spark
	 * Configuration.
	 * 
	 * @param appName
	 *            Name of the Spark Application
	 * @param master
	 *            Specifies where the Spark Application runs. Takes values
	 *            'local', local[*], 'master'.
	 * @return JavaSparkContext.           
	 */
	public JavaSparkContext context(String appName, String master) {

		/* Creating Spark Configuration */
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

		/* Creating Spark Context */
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		return sparkContext;

	}
	
	/**
	 * Function 'streaming()' creates a JavaStreamingContext. 
	 * @param sparkContext JavaSparkContext.
	 * @param seconds Batch Duration.
	 * @return JavaStreamingContext
	 */
	public JavaStreamingContext streaming(JavaSparkContext sparkContext, long seconds){

		/* Creating Spark Streaming Context. */
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext,Durations.seconds(seconds));
				
		return streamingContext;
	}

}
