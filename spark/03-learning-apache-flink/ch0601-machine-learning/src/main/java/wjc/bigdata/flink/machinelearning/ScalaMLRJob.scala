package com.demo.flink.ml

import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.regression.MultipleLinearRegression

object ScalaMLRJob {
    def main(args: Array[String]) {
        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment


        val trainingDataset = MLUtils.readLibSVM(env, "iris-train.txt")
        val testingDataset = MLUtils.readLibSVM(env, "iris-test.txt").map { lv => lv.vector }
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
