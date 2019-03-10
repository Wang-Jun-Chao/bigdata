package wjc.bigdata.flink.complexeventprocessing;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class MessageProducer extends Base {
    private final static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final static KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(KAFKA_PROPS);

    public static void main(String[] args) {

        String[] data = {
                "xyz=21.0",
                "xyz=30.0",
                "LogShaft=29.3",
                "Boiler=23.1",
                "Boiler=24.2",
                "Boiler=27.0",
                "Boiler=29.0"
        };

        try {

            while (true) {
                for (String s : data) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            PRODUCT_TOPIC, "id" + System.currentTimeMillis(), s);
                    RecordMetadata metadata = PRODUCER.send(record).get();
                    logger.warn(metadata + " --> " + record.key() + ": " + record.value());
                    TimeUnit.MILLISECONDS.sleep(2000);
                }

                TimeUnit.MILLISECONDS.sleep(1000);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
