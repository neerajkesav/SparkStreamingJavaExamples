## Spark Streaming Java Examples

This project is created to learn Apache Spark Streaming using Java. This project consists of the following examples:

  * Streaming from a socket.

### Data Sets
 * Some random text.
 
### Getting Started

These instructions will get you a brief idea on setting up the environment and running on your local machine for development and testing purposes. 

**Prerequisities**

- Java
- Apache Spark 
- Hadoop

**Setup and running tests**

1. Run `javac` and `java -version` to check the installation
   
2. Run `spark-shell` and check if Spark is installed properly. 
 
3. Go to Hadoop user (If  installed on different user) and run the following (On Ubuntu Systems): 

      `sudo su hadoopuser`
          
      `start-all.sh`   
           
4. Execute the following commands from terminal to run the tests:

      `javac -classpath "Path to required jar files(spark, hadoop, scala)" Main.java`
      
      `java -classpath "Path to required jar files(spark, hadoop, scala)" Main`


###Classes
Please start exploring from Main.java

All classes in this project are listed below:

* **CreateSpark.java** - To create SparkContext and StreamingContext. Contains the following methods:
	
    `public JavaSparkContext context(String appName, String master)`
    `public JavaStreamingContext streaming(JavaSparkContext sparkContext, long seconds)`

* **Streaming.java** - Examples on Spark Streaming and using the data for Spark Transformations. Contains the following methods.
	
	`public static void wordCount(JavaStreamingContext streamingContext, long timeout)`
	`public static void lineCount(JavaStreamingContext streamingContext, long timeout)`
	`public static void windowCount(JavaStreamingContext streamingContext, long timeout)`
	
* **Main.java** - Main class to test and run the classes in this project.






