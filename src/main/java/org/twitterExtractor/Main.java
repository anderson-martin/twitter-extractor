package org.twitterExtractor;

/**
 * Created by Martin Anderson
 */

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;





public class Main {
  /**
  Tuning the Spark by serialization
   */
  private static final Class[] classesInKryo = ImmutableList.builder()
      .add(GeoLocation.class)
      .add(Status.class)
      .add(User.class)
      .build()
      .toArray(new Class[] {});

  /**
  Shows the outut messages
   */
  private static final Logger printer = LoggerFactory.getLogger(Main.class);


  public static void main(String args[]) throws InterruptedException {
    new Main().run();
  }

  public void run() throws InterruptedException {


    SparkConf sparkConfiguration = new SparkConf().setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(classesInKryo)
        .setAppName("sparkTask");

    /*
     set the time interval between batches
     */
    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConfiguration, Durations.seconds(1));


    /*

    Using customer reciever and shwoing the output

     */
    streamingContext.receiverStream(new CustomReciever(StorageLevel.MEMORY_ONLY()))
        .foreachRDD(
                // filtering empty parittions
            rdd -> rdd.coalesce(10)
                .foreach(message -> printer.info(message.getText())));


    // start computation
    streamingContext.start();

    streamingContext.awaitTermination();
  }
}
