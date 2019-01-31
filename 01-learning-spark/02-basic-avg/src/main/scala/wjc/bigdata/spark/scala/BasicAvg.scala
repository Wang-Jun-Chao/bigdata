package wjc.bigdata.spark.scala

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author: wangjunchao(王俊超)
  * @time: 2019-01-17 13:36
  **/
object BasicAvg {

    def comupteAvg(input: RDD[Int]): (Int, Int) = {
        input.aggregate((0, 0))(
            (x, y) => (x._1 + y, x._2 + 1),
            (x, y) => (x._1 + y._1, x._2 + y._2)
        )
    }

    def main(args: Array[String]): Unit = {
        val master = args.length match {
            case x: Int if x > 0 => args(0)
            case _ => "local"
        }

        val ctx = new SparkContext(master, "basic-avg",
            System.getenv("SPARK_HOME"))
        val input = ctx.parallelize(List(1, 2, 3, 4))
        val result = comupteAvg(input)
        val avg = result._1 / result._2
        print(avg)
    }
}
