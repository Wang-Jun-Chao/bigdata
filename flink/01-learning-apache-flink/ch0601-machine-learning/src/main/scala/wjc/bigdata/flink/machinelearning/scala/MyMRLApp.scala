//package wjc.bigdata.flink.machinelearning.scala
//
//import org.apache.flink.api.scala._
//import org.apache.flink.ml.common.LabeledVector
//import org.apache.flink.ml.regression.MultipleLinearRegression
//
///**
//  * @author: wangjunchao(王俊超)
//  * @time: 2019-03-03 21:14
//  **/
//object MyMRLApp {
//
//    def main(args: Array[String]): Unit = {
//        val env = ExecutionEnvironment.getExecutionEnvironment
//
//        // Create multiple linear regression learner
//        val mlr = MultipleLinearRegression()
//            .setIterations(10)
//            .setStepsize(0.5)
//            .setConvergenceThreshold(0.001)
//
//        // Obtain training and testing data set
//        val trainingDS: DataSet[LabeledVector] = // input data
//        // val testingDS: DataSet[Vector] = // output data
//
//        // Fit the linear model to the provided data
//            mlr.fit(trainingDS)
//
//        // Calculate the predictions for the test data
//        //    val predictions = mlr.predict(testingDS)
//        predictions.writeAsText("mlr-out")
//
//        env.execute("Flink MLR App")
//}
