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
    println("why again so many problem")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap 
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
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