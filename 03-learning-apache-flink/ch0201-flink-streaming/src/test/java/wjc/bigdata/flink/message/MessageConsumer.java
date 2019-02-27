package wjc.bigdata.flink.message;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.flink.streaming.Base;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class MessageConsumer extends Base {
    private final static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);



    public static void main(String[] args) {

        KAFKA_PROPS.put("client.id", "kafka-streaming-output");
        KafkaConsumer<String, String> CONSUMER = new KafkaConsumer<>(KAFKA_PROPS);

        // 向集群请求主题可用的分区。如果只打算读取特定分区，可以跳过这一步。
        List<PartitionInfo> partitionInfos = CONSUMER.partitionsFor(CONSUME_TOPIC);
        List<TopicPartition> partitions = new ArrayList<>();
        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
            // 知道需要哪些分区之后，调用assign()方位。
            CONSUMER.assign(partitions);

            while (true) {
                ConsumerRecords<String, String> records = CONSUMER.poll(1000L);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + ": " + record.value());
                }
            }
        }
    }
}
