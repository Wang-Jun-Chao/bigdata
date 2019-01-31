package wjc.bigdata.spark.data.pump.connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 15:58
 **/
public class KafkaOutput implements Output {
    private final static Logger logger = LoggerFactory.getLogger(KafkaOutput.class);

    private Map<String, String> kafkaConfig;
    private KafkaProducer       producer;
    private List<String>        topics;

    public KafkaOutput(Map<String, String> kafkaConfig, List<String> topics) {
        this.kafkaConfig = kafkaConfig;
        this.topics = topics;
        this.producer = new KafkaProducer(kafkaConfig);
    }

    @Override
    public <T> void write(RDD<T> rdd) {

    }

    @Override
    public <T> void write(JavaRDDLike<T, ? extends JavaRDDLike> rdd) {
        producer.initTransactions();

        try {
            producer.beginTransaction();

            rdd.foreachPartition(new VoidFunction<Iterator<T>>() {
                @Override
                public void call(Iterator iterator) throws Exception {
                    iterator.forEachRemaining(item -> topics.forEach(topic -> producer.send(new ProducerRecord(topic, item))));
                }
            });

            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }
}
