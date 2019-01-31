package wjc.bigdata.spark.cluster;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import wjc.bigdata.spark.data.pump.operation.Handler;
import wjc.bigdata.spark.data.pump.operation.HandlerChain;
import wjc.bigdata.spark.data.pump.connector.KafkaInput;
import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:50
 **/
public class KafkaHandler implements Handler {
    private final static Logger logger = LoggerFactory.getLogger(KafkaHandler.class);

    @Override
    public void handle(Input inputConnector, Output output, HandlerChain chain) {
        KafkaInput kafkaReader;
        if (inputConnector !=null && KafkaInput.class.isAssignableFrom(inputConnector.getClass())) {
            kafkaReader = (KafkaInput) inputConnector;
            JavaInputDStream stream = kafkaReader.getJavaInputDStream();
            stream.mapToPair((PairFunction) object -> {

                if (ConsumerRecord.class.isAssignableFrom(object.getClass())) {
                    ConsumerRecord record = (ConsumerRecord) object;
                    return new Tuple2(record.key(), record.value());
                }
                return new Tuple2(null, object);
            });
        } else {

        }
    }
}
