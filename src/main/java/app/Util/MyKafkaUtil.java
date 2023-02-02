package app.Util;

import app.common.EPConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    public static FlinkKafkaProducer<String> getKafkaProduce(String topic) {
        return new FlinkKafkaProducer<String>(EPConfig.BROKER, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupID) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EPConfig.BROKER);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
}