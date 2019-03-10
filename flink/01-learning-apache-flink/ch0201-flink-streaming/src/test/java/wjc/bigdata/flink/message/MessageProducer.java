package wjc.bigdata.flink.message;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.flink.streaming.Base;

import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class MessageProducer extends Base {
    private final static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final static KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(KAFKA_PROPS);

    public static void main(String[] args) {

        Random random = new Random();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {

            while (true) {
                int id = random.nextInt();
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        PRODUCT_TOPIC, "" + System.currentTimeMillis(), System.currentTimeMillis()
                        + "," + (int) (Math.random() * 100) + ",sensor-id-" + (int) (Math.random() * 100));

                RecordMetadata metadata = PRODUCER.send(record).get();
                logger.warn(metadata + " --> " + record.key() + ": " + record.value());

                TimeUnit.MICROSECONDS.sleep(100);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
