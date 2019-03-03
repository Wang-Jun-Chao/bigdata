package wjc.bigdata.flink.machinelearning.scala

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.ml.regression.MultipleLinearRegression
import wjc.bigdata.flink.util.PathUtils

/**
  * @author: wangjunchao(王俊超)
  * @time: 2019-03-03 21:14
  **/
object Job {
    def main(args: Array[String]) {
        // set up the execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment

        val filePath = PathUtils.workDir("iris.csv")

        val iriscsv = env.readCsvFile[(String, String, String, String, String)](filePath)


        val irisLV = iriscsv
            .map { tuple =>
                val list = tuple.productIterator.toList
                val numList = list.map(_.asInstanceOf[String].toDouble)
                LabeledVector(numList(4), DenseVector(numList.take(4).toArray))
            }


        //  irisLV.print
        // val trainTestData = Splitter.trainTestSplit(irisLV)
        val trainTestData = Splitter.trainTestSplit(irisLV, .6, true)
        val trainingData: DataSet[LabeledVector] = trainTestData.training

        val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)

        testingData.print()

        val mlr = MultipleLinearRegression()
            .setStepsize(1.0)
            .setIterations(5)
            .setConvergenceThreshold(0.001)

        mlr.fit(trainingData)

        // The fitted model can now be used to make predictions
        val predictions = mlr.predict(testingData)

        predictions.print()
    }
}
