package wjc.bigdata.spark.data.pump.operation;

import wjc.bigdata.spark.data.pump.connector.KafkaInput;

import java.util.Optional;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 19:24
 **/
public class KafkaAfterAction implements AfterAction {
    private KafkaInput reader;

    public KafkaAfterAction() {
    }

    public KafkaAfterAction(KafkaInput reader) {
        this.reader = reader;
    }

    public KafkaInput getReader() {
        return reader;
    }

    public void setReader(KafkaInput reader) {
        this.reader = reader;
    }

    @Override
    public void act() {
        Optional.ofNullable(reader).ifPresent(reader -> {
            reader.getJavaStreamingContext().start();
            reader.getJavaStreamingContext();
        });
    }
}
