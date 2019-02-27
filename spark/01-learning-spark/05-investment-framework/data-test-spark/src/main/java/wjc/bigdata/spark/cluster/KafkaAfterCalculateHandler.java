package wjc.bigdata.spark.cluster;

import wjc.bigdata.spark.data.pump.connector.spark.KafkaInput;
import wjc.bigdata.spark.data.pump.operation.AfterCalculateHandler;

import java.util.Optional;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 19:24
 **/
public class KafkaAfterCalculateHandler implements AfterCalculateHandler {
    private KafkaInput reader;

    public KafkaAfterCalculateHandler() {
    }

    public KafkaAfterCalculateHandler(KafkaInput reader) {
        this.reader = reader;
    }

    public KafkaInput getReader() {
        return reader;
    }

    public void setReader(KafkaInput reader) {
        this.reader = reader;
    }

    @Override
    public void handle() {
        Optional.ofNullable(reader).ifPresent(reader -> {
            try {
                reader.getJavaStreamingContext().start();
                reader.getJavaStreamingContext().awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
