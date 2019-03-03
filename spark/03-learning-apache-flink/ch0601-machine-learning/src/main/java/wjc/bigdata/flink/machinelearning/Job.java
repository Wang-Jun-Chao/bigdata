package wjc.bigdata.flink.machinelearning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-03 19:46
 **/
public class Job {
    private final static Logger logger = LoggerFactory.getLogger(Job.class);

    public static void main(String[] args) {
        // set up the execution environment


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = Job.class.getClassLoader().getResource("").getPath() + "iris.csv";
        CsvReader csvReader = env.readCsvFile(filePath);

        DataSource<Tuple5<Double, Double, Double, Double, Double>> dataSource = csvReader.types(
                Double.class, Double.class, Double.class, Double.class, Double.class);
        MapOperator<Tuple5<Double, Double, Double, Double, Double>, LabeledVector> mapOperator = dataSource.map(
                new MapFunction<Tuple5<Double, Double, Double, Double, Double>, LabeledVector>() {
                    @Override
                    public LabeledVector map(Tuple5<Double, Double, Double, Double, Double> value) throws Exception {
                        return new LabeledVector(value.f0, new DenseVector(new double[]{
                                value.f1, value.f2, value.f3, value.f4
                        }));
                    }
                });


        //  irisLV.print
//        Splitter.trainTestSplit(mapOperator., 0.6, true, TypeInformation.of(DataSet.class))

    }

//                .map() {
//            tuple =>
//            val list = tuple.productIterator.toList
//            val numList = list.map(_.asInstanceOf[String].toDouble)
//            LabeledVector(numList(4), DenseVector(numList.take(4).toArray))


    //  irisLV.print
    // val trainTestData = Splitter.trainTestSplit(irisLV)
//        val trainTestData = Splitter.trainTestSplit(irisLV, .6, true)
//        val trainingData:DataSet[LabeledVector] = trainTestData.training
//
//        val testingData:DataSet[Vector] = trainTestData.testing.map(lv = > lv.vector)
//
//        testingData.print()
//
//        val mlr = MultipleLinearRegression()
//                .setStepsize(1.0)
//                .setIterations(5)
//                .setConvergenceThreshold(0.001)
//
//        mlr.fit(trainingData)
//
//        // The fitted model can now be used to make predictions
//        val predictions = mlr.predict(testingData)
//
//        predictions.print()
}
