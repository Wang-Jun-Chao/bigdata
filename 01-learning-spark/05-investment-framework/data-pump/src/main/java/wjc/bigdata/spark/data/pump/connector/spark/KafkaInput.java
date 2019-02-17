package wjc.bigdata.spark.data.pump.connector.spark;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.AbstractInput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 14:37
 **/
public class KafkaInput extends SparkInput {
    private final static Logger logger = LoggerFactory.getLogger(KafkaInput.class);

    private final static String PREFIX            = KafkaInput.class.getSimpleName();
    private final static String SPARK_CONFIG_ATTR = PREFIX + ".sparkConfig";
    private final static String KAFKA_CONFIG_ATTR = PREFIX + ".kafkaConfig";
    private final static String DURATION_ATTR     = PREFIX + ".duration";
    private final static String TOPICS_ATTR       = PREFIX + ".topics";
    private final static String CONTEXT_ATTR      = PREFIX + ".javaStreamingContext";
    private final static String DSTREAM_ATTR      = PREFIX + ".javaInputDStream";

    public KafkaInput() {
    }

    public KafkaInput(Map<String, String> sparkConfig,
                      Map<String, String> kafkaConfig,
                      Duration duration,
                      List<String> topics) {
        setSparkConfig(sparkConfig);
        setKafkaConfig(kafkaConfig);
        setDuration(duration);
        setTopics(topics);

        init();
    }


    public void init() {
        SparkConf sparkConf = new SparkConf();
        getSparkConfig().forEach(sparkConf::set);

        final JavaStreamingContext context = new JavaStreamingContext(sparkConf, getDuration());
        final JavaInputDStream<ConsumerRecord<Object, Object>> stream = KafkaUtils.createDirectStream(
                context,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(getTopics(), new HashMap<>(getKafkaConfig()))
        );

        setJavaStreamingContext(context);
        setJavaInputDStream(stream);
    }

    public Map<String, String> getSparkConfig() {
        return (Map<String, String>) getAttribute(SPARK_CONFIG_ATTR);
    }

    public void setSparkConfig(Map<String, String> sparkConfig) {
        Objects.requireNonNull(sparkConfig, "spark config should not be null");
        setAttribute(SPARK_CONFIG_ATTR, sparkConfig);
    }

    public Map<String, String> getKafkaConfig() {
        return (Map<String, String>) getAttribute(KAFKA_CONFIG_ATTR);
    }

    public void setKafkaConfig(Map<String, String> kafkaConfig) {
        Objects.requireNonNull(kafkaConfig, "kafka config should not be null");
        setAttribute(KAFKA_CONFIG_ATTR, kafkaConfig);
    }

    public Duration getDuration() {
        return (Duration) getAttribute(DURATION_ATTR);
    }

    public void setDuration(Duration duration) {
        Objects.requireNonNull(duration, "duration should not be null");
        setAttribute(DURATION_ATTR, duration);
    }

    public List<String> getTopics() {
        return (List<String>) getAttribute(TOPICS_ATTR);
    }

    public void setTopics(List<String> topics) {
        Objects.requireNonNull(topics, "topics should not be null");
        setAttribute(TOPICS_ATTR, topics);
    }

    @Override
    public JavaStreamingContext getJavaStreamingContext() {
        return (JavaStreamingContext) getAttribute(CONTEXT_ATTR);
    }



    public void setJavaStreamingContext(JavaStreamingContext context) {
        Objects.requireNonNull(context, "context should not be null");
        setAttribute(CONTEXT_ATTR, context);
    }

    public JavaInputDStream getJavaInputDStream() {
        return (JavaInputDStream) getAttribute(DSTREAM_ATTR);
    }

    public void setJavaInputDStream(JavaInputDStream stream) {
        Objects.requireNonNull(stream, "stream should not be null");
        setAttribute(DSTREAM_ATTR, stream);
    }


    @Override
    public JavaRDDLike readJavaRDD() {
        return getJavaRDD();
    }

    @Override
    public Collection<? extends JavaRDDLike> batchReadJavaRDD() {
        JavaRDDLike rdd = readJavaRDD();
        List<JavaRDDLike> result = new ArrayList<>();
        result.add(rdd);
        return result;
    }

    @Override
    public RDD read() {
        return null;
    }

    @Override
    public Collection<? extends RDD> batchRead() {
        return null;
    }
}
