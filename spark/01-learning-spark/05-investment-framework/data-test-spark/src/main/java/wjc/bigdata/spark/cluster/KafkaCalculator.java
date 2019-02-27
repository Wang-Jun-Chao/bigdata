package wjc.bigdata.spark.cluster;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import wjc.bigdata.spark.data.pump.connector.Output;
import wjc.bigdata.spark.data.pump.connector.spark.KafkaInput;
import wjc.bigdata.spark.data.pump.connector.spark.KafkaOutput;
import wjc.bigdata.spark.data.pump.operation.CalculationContext;
import wjc.bigdata.spark.data.pump.operation.Calculator;
import wjc.bigdata.spark.data.pump.operation.CalculatorChain;

import java.util.Iterator;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:50
 **/
public class KafkaCalculator implements Calculator {
    private final static Logger logger = LoggerFactory.getLogger(KafkaCalculator.class);

    @Override
    public void handle(CalculationContext context, CalculatorChain chain) {
        KafkaInput kafkaReader;
        if (context.getInput() != null && KafkaInput.class.isAssignableFrom(context.getInput().getClass())) {
            kafkaReader = (KafkaInput) context.getInput();
            JavaInputDStream stream = kafkaReader.getJavaInputDStream();
            JavaPairDStream<String, String> pairDStream = stream.mapToPair((PairFunction) object -> {

                if (ConsumerRecord.class.isAssignableFrom(object.getClass())) {
                    ConsumerRecord record = (ConsumerRecord) object;
                    return new Tuple2(record.key(), record.value());
                }
                return new Tuple2(null, object);
            });

            Output output = context.getOutput();
            if (output!= null && KafkaOutput.class.isAssignableFrom(output.getClass())) {
                KafkaOutput kafkaOutput = (KafkaOutput) output;

            }

            pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
                @Override
                public void call(JavaPairRDD<String, String> pairRDD) throws Exception {

                    Output output = context.getOutput();
                    if (output!= null && KafkaOutput.class.isAssignableFrom(output.getClass())) {
                        KafkaOutput kafkaOutput = (KafkaOutput) output;
                        kafkaOutput.writeJavaRDD(pairRDD);
                    }
                }
            });

            pairDStream.print();
        } else {
            logger.error("can not get KafkaInput instance");
        }
    }
}
