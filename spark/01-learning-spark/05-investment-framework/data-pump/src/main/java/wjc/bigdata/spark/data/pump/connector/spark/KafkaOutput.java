package wjc.bigdata.spark.data.pump.connector.spark;

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
import scala.Tuple2;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 15:58
 **/
public class KafkaOutput<T> extends SparkOutput {
    private final static Logger logger = LoggerFactory.getLogger(KafkaOutput.class);

    private Map<String, String> kafkaConfig;
    //    private KafkaProducer       producer;
    private List<String>        topics;

    public KafkaOutput() {
    }

    public KafkaOutput(Map<String, String> kafkaConfig, List<String> topics) {
        this.kafkaConfig = kafkaConfig;
        this.topics = topics;
        init();
    }

    public void init() {
//        this.producer = new KafkaProducer(kafkaConfig);
    }

    @Override
    public void write(RDD rdd) {

    }

    @Override
    public void write(Collection<? extends RDD> data) {

    }

    @Override
    public void writeJavaRDD(JavaRDDLike rdd) {


        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {

                KafkaProducer producer = new KafkaProducer(kafkaConfig);
                producer.initTransactions();
                try {
                    producer.beginTransaction();
                    iterator.forEachRemaining(item -> topics.forEach(topic -> producer.send(new ProducerRecord(topic, item._1, item._2))));
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
        });


    }

    @Override
    public void writeJavaRDD(Collection<? extends JavaRDDLike> data) {
        data.forEach(this::writeJavaRDD);
    }

    /////////////////////
    // setters and getters
    /////////////////////


    public Map<String, String> getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(Map<String, String> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public KafkaProducer getProducer() {
//        return producer;
        return null;
    }

    public void setProducer(KafkaProducer producer) {
//        this.producer = producer;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
}
