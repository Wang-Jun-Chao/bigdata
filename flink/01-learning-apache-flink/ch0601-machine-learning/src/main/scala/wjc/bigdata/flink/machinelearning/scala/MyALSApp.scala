package wjc.bigdata.flink.machinelearning.scala

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import wjc.bigdata.flink.util.PathUtils

/**
  * @author: wangjunchao(王俊超)
  * @time: 2019-03-03 21:14
  **/
object MyALSApp {
    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int, Int, Double)](
            PathUtils.workDir("books.csv"))

        // Setup the ALS learner
        val als = ALS()
            .setIterations(10)
            .setNumFactors(10)
            .setBlocks(100)
            .setTemporaryPath(PathUtils.workDir("temp"))

        // Set the other parameters via a parameter map
        val parameters = ParameterMap()
            .add(ALS.Lambda, 0.9)
            .add(ALS.Seed, 42L)

        // Calculate the factorization
        als.fit(inputDS, parameters)

        // Read the testing data set from a csv file
        val testingDS: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)](
            PathUtils.workDir("books-test.csv"))

        // Calculate the ratings according to the matrix factorization
        val predictedRatings = als.predict(testingDS)

        predictedRatings.writeAsCsv(PathUtils.workDir("books-output"))

        env.execute("Flink Recommendation App")
    }
}
