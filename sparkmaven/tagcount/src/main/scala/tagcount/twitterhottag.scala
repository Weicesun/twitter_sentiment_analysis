package tagcount
import org.apache.spark._;

object twitterhottag {
  def main(args : Array[String]) {
    val sc = new SparkContext("local", "word", "usr/local/spark",
        Nil, Map(), Map());
    val input = sc.textFile("input.txt");
    val count = input.flatMap(line => line.split(" "))
    .map(word=>(word, 1))
    .reduceByKey(_+_)
    
    count.saveAsTextFile("output")
    System.out.println("ok")
  }
  
}