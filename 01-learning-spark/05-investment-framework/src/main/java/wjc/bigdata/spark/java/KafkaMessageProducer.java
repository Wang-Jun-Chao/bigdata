package wjc.bigdata.spark.java;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-23 17:01
 **/
public class KafkaMessageProducer extends Base {
    private final static Logger logger = LoggerFactory.getLogger(KafkaMessageProducer.class);


    private static KafkaMessageProducer instance = null;

    private Producer producer;

    private KafkaMessageProducer(String brokerList) {

        KAFKA_PROPS.put("bootstrap.servers", brokerList);
        KAFKA_PROPS.put("client.id", "kafka-streaming-output");
        this.producer = new KafkaProducer(KAFKA_PROPS);
    }

    public static synchronized KafkaMessageProducer getInstance(String brokerList) {
        if (instance == null) {
            instance = new KafkaMessageProducer(brokerList);
            System.out.println("init kafka message producer...");
        }
        return instance;
    }

    // 单条发送
    public void send(ProducerRecord<String, String> keyedMessage) {
        producer.send(keyedMessage);
    }

//    // 批量发送
//    public void send(List<ProducerRecord<String, String>> keyedMessageList) {
//        producer.send(keyedMessageList);
//    }

    public void shutdown() {
        producer.close();
    }

}
