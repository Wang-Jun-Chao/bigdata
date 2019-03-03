package wjc.bigdata.flink.machinelearning.scala

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.regression.MultipleLinearRegression
import wjc.bigdata.flink.util.PathUtils

/**
  * @author: wangjunchao(王俊超)
  * @time: 2019-03-03 21:14
  **/
object MLRJob {
    def main(args: Array[String]) {
        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment


        val trainingDataset = MLUtils.readLibSVM(env, PathUtils.workDir("iris-train.txt"))
        val testingDataset = MLUtils.readLibSVM(env, PathUtils.workDir("iris-test.txt")).map { lv => lv.vector }
        val mlr = MultipleLinearRegression()
            .setStepsize(1.0)
            .setIterations(5)
            .setConvergenceThreshold(0.001)

        mlr.fit(trainingDataset)

        // The fitted model can now be used to make predictions
        val predictions = mlr.predict(testingDataset)

        predictions.print()

    }
}
