package wjc.bigdata.spark.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(SendMessage.class);

    private final static KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(KAFKA_PROPS);

    public static void main(String[] args) {

        Random random = new Random();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {

            while (true) {

                int id = random.nextInt();
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        TOPIC, "wjc-" + System.currentTimeMillis(), "val-" + sdf.format(new Date()));

                RecordMetadata metadata = PRODUCER.send(record).get();
                logger.warn(metadata + " --> " +  record.key() + ": " + record.value());

                TimeUnit.SECONDS.sleep(2);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
