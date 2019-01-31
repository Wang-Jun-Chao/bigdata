package wjc.bigdata.spark.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 10:36
 **/
public class KafkaStreaming extends Base {
    private final static Logger logger = LoggerFactory.getLogger(KafkaStreaming.class);

    public static void main(String[] args) throws InterruptedException {
        SPARK_CONF.setAppName("kafka-stream");

        // 设置brokers
        if (args != null && args.length > 0) {
            KAFKA_PROPS.put("bootstrap.servers", args[0]);
        }

        // 设置master
        if (args != null && args.length > 1) {
            SPARK_CONF.setMaster(args[1]);
        }


        JavaStreamingContext jsc = new JavaStreamingContext(SPARK_CONF, Durations.seconds(8));

        final Broadcast<String> brokersBroadcast = jsc.sparkContext().broadcast(BROKERS);
        final Broadcast<String> topicBroadcast = jsc.sparkContext().broadcast(TOPIC);


        // 要监听的Topic，可以同时监听多个
        Collection<String> topics = Collections.singletonList(TOPIC);

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, KAFKA_PROPS)
        );


        JavaPairDStream<String, String> message = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                });


        message.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> v) throws Exception {


                v.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                        KafkaMessageProducer kafkaMessageProducer = KafkaMessageProducer.getInstance(
                                brokersBroadcast.getValue());
                        ProducerRecord<String, String> record;
                        Tuple2<String, String> tuple2;
                        while (iterator.hasNext()) {
                            tuple2 = iterator.next();
                            System.out.println(tuple2);
                            record = new ProducerRecord<>(topicBroadcast.getValue() + "-out", tuple2._1, tuple2._2);
                            kafkaMessageProducer.send(record);
                        }
                    }
                });


            }
        });

        message.print();

        jsc.start();
        jsc.awaitTermination();
    }
}
