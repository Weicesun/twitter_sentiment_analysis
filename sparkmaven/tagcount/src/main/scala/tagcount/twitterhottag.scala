package tagcount
import java.util.HashMap;
import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext._;
import org.apache.kafka.clients.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
object twitterhottag {
  def main(args : Array[String]) {
    if (args.length < 4) {
        System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
        System.exit(1);
    }
    
   // StreamingExamples.setStreamingLogLevels();
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("countTag").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap 
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val topCounts60 = words.map((_, 1)).reduceByKeyAndWindow(_+_, Seconds(60))
    .map{case(topic, count) => (count, topic)}
    .transform(_.sortByKey(false))
    topCounts60.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\n Popular tag in 60 seconds (%s total)".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s(%s tweets)".format(tag, count))}
        })
    //val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    //wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}