package org.example.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.SensorReading2;


/**
 * 延迟数据3重保障
 * <p>
 * 1.watermark
 * 2.窗口迟到处理
 * 3.侧输出流
 */
public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 在代码中设置 EventTime
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<SensorReading2> socketSource = env.socketTextStream("127.0.0.1", 9999)
                .flatMap(new FlatMapFunction<String, SensorReading2>() {
                    @Override
                    public void flatMap(String value, Collector<SensorReading2> out) throws Exception {
                        String[] split = value.split("\\s+");
                        if (split.length == 3) {
                            out.collect(new SensorReading2(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])));
                        }
                    }
                });

        // 侧输出
        OutputTag<SensorReading2> side_01 = new OutputTag<SensorReading2>("side_01"){};

        // 处理无序数据的水位
        // 第一个窗口: (1619335890,1619335905],触发时机 1619335907 数据来的时候(1619335905+水位2)
        SingleOutputStreamOperator<SensorReading2> outputStreamOperator = socketSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading2>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading2 element) {
                        return element.getTimestamp() * 1000L;
                    }
                }).keyBy(SensorReading2::getId)
                .timeWindow(Time.seconds(15))
                // 运行窗口允许延迟1分钟
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(side_01)
                .minBy("temperature");
        outputStreamOperator.print("乱序处理");
        outputStreamOperator.getSideOutput(side_01).print("side_01");


        // 处理有序数据
        // 第一个窗口: (1619335890,1619335905] 触发时机 1619335905
        socketSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading2>() {
            @Override
            public long extractAscendingTimestamp(SensorReading2 element) {
                return element.getTimestamp() * 1000L;
            }
        }).keyBy(SensorReading2::getId).timeWindow(Time.seconds(15))
                .minBy("temperature")
        //.print()
        ;


        // TimestampAssigner
        // AssignerWithPeriodicWatermarks 周期性的生成 watermark，比如200ms生成一个
        // AssignerWithPunctuatedWatermarks 没有时间周期规律，可打断的生成 watermark

        env.execute();


    }

}
