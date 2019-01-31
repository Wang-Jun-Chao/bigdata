package wjc.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: wangjunchao(王俊超)
  * @time: 2019-01-16 17:11
  **/
object WordCount {
    def main(args: Array[String]) {
        val inputFile = args(0)
        val outputFile = args(1)
        val conf = new SparkConf().setAppName("word-count")
        // Create a Scala Spark Context.
        val sc = new SparkContext(conf)
        // Load our input data.
        val input = sc.textFile(inputFile)
        // Split up into words.
        val words = input.flatMap(line => line.split(" "))
        // Transform into word and count.
        val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
        // Save the word count back out to a text file, causing evaluation.
        counts.saveAsTextFile(outputFile)
    }
}
