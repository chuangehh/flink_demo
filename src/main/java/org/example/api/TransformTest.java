package org.example.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class TransformTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        keyBy(env);

        env.execute();
    }

    private static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
        ));

        // max只会处理max的字段, 与之相关联的其他行不会更新
        source.keyBy(SensorReading::getId).max("temperature").print("max");

        // 2> SensorReading(id=guangzhou, timestamp=2021-04-02, temperature=24.2)
        // 2> SensorReading(id=guangzhou, timestamp=2021-04-02, temperature=24.2)
        source.keyBy(SensorReading::getId).maxBy("temperature").print("maxBy");

        // maxBy只能根据一个字段进行聚合, 理论上应该取timestamp=2021-04-03 才对，怎么实现呢？
        // 用reduce
        source.keyBy(SensorReading::getId).reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading t1, SensorReading newData) {
                return new SensorReading(t1.getId(), newData.getTimestamp(), Math.max(t1.getTemperature(), newData.getTemperature()));
            }
        }).print("reduce");
    }

}
