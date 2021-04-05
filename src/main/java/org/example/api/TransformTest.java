package org.example.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Collections;

/**
 * 转换API
 *
 * @author liangchuanchuan
 */
public class TransformTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
                , new SensorReading("guangzhou", "2021-04-05", 22.6)
        ));

        keyBy(source);

        splitSelect(env, source);

        env.execute();
    }

    /**
     * 分流合流
     *
     * @param env
     * @param source
     */
    private static void splitSelect(StreamExecutionEnvironment env, DataStream<SensorReading> source) {
        // split select
        // 1.根据温度切分成2条流
        String high = "high";
        String low = "low";
        SplitStream<SensorReading> split = source.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                if (value.getTemperature() > 30) {
                    return Arrays.asList(high);
                }
                return Arrays.asList(low);
            }
        });

        DataStream<SensorReading> highSelect = split.select(high);
        DataStream<SensorReading> lowSelect = split.select(low);
        highSelect.print(high);
        lowSelect.print(low);

        // connect , map , flatMap
        // 2.高温报warn,低温报info
        highSelect.connect(lowSelect).map(new CoMapFunction<SensorReading, SensorReading, Object>() {
            @Override
            public Object map1(SensorReading value) throws Exception {
                return new Tuple3<String, Double, String>(value.getId(), value.getTemperature(), "warn");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<String, String>(value.getId(), "info");
            }
        }).print("CoMap");

        // union
        DataStreamSource<SensorReading> shanghai = env.fromCollection(Collections.singletonList(new SensorReading("shanghai", "2021-04-05", 32.7)));
        highSelect.union(lowSelect).union(shanghai).print("union");
    }

    private static void keyBy(DataStream<SensorReading> source) {


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
