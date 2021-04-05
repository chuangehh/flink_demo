package org.example.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.SensorReading;

import java.util.Arrays;

public class PartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
                , new SensorReading("guangzhou", "2021-04-05", 22.6)
        ));


        // shuffle 随机分配
        // random.nextInt(numberOfChannels)
        source.shuffle().print("shuffle");

        // global 只给第一个下游
        // return 0;
        source.global().print("global");

        // 根据 key分区
        // keyGroupId * parallelism / maxParallelism
        source.keyBy(SensorReading::getId).print("keyBy");

        // TODO 自定义分区..

        env.execute();
    }

}
