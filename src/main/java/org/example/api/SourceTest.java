package org.example.api;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 1.从集合中读取数据
        DataStreamSource<Object> stream1 = env.fromCollection(Arrays.asList(
                new Person("Fred", 35),
                new Person("Fred", 35),
                new Person("Fred", 35),
                new Person("Fred", 35),
                new Person("Fred", 35),
                new Person("Fred", 35),
                new Person("Pebbles", 2)));
        stream1.print("stream1:");

        // 2.从文件中读取数据
        DataStreamSource<String> stream2 = env.readTextFile("D:\\javaProject\\flink_demo\\src\\main\\resources\\word.data");
        stream2.print("stream2:");

        // 3.从Kafka读数据
        // [kafka@node1 ~]$ /usr/local/kafka_2.11-2.0.0/bin/kafka-console-producer.sh --topic flink.person
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.124:9092");
        properties.setProperty("group.id", "flink-01");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> stream3 = env.addSource(new FlinkKafkaConsumer<String>("flink.person", new SimpleStringSchema(), properties));
        stream3.print("stream3:");


        env.execute("stream");
    }

}
