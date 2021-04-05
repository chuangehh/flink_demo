package org.example.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * kafka 输入，输出
 *
 * @author liangchuanchuan
 */
public class KafkaSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // [node1 ~]$ /usr/local/kafka_2.11-2.0.0/bin/kafka-console-producer.sh --topic flink.person --broker-list bgnode17:9092
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.124:9092");
        properties.setProperty("group.id", "flink-01");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> kakfaSource = env.addSource(new FlinkKafkaConsumer<String>("flink.person", new SimpleStringSchema(), properties));


        // [node1 ~]$ /usr/local/kafka_2.11-2.0.0/bin/kafka-console-consumer.sh --bootstrap-server bgnode17:9092 --topic flink.person_output
        kakfaSource.map(new MapFunction<String, String>() {
            int i = 0;

            @Override
            public String map(String value) throws Exception {
                return Tuple2.of(value, i++).toString();
            }
        }).addSink(new FlinkKafkaProducer<>("192.168.1.124:9092", "flink.person_output", new SimpleStringSchema()));

        env.execute("stream");
    }

}
