package tagcount
import java.util.*;

import org.apache.spark._;

object twitterhottag {
  def main(args : Array[String]) {
    if (args.length < 4) {
        System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
        System.exit(1);
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("countTag")
    val scc = new StreamContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap 
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_,_2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x, 1L))
    .reduceByKeyAndWindow(_+_, _-_, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    /*
    val sc = new SparkContext("local", "word", "usr/local/spark",
        Nil, Map(), Map());
    val input = sc.textFile("input.txt");
    val count = input.flatMap(line => line.split(" "))
    .map(word=>(word, 1))
    .reduceByKey(_+_)
    
    count.saveAsTextFile("output")
    System.out.println("ok")*/
  }
  
}