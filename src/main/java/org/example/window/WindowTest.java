package org.example.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import org.apache.http.client.utils.DateUtils;
import org.example.bean.SensorReading;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Date;

public class WindowTest {

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


        windowType(source);
        windowFunction(env, source);

        env.execute();
    }

    private static void windowFunction(StreamExecutionEnvironment env, DataStream<SensorReading> source) {
        // 窗口函数
        // 1.增量聚合函数
        // 1.1 ReduceFunction

        source.print("source");
        source.keyBy(SensorReading::getId)
                .countWindow(5).reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                // 取天数的最大温度
                if (t1.getTemperature() > sensorReading.getTemperature()) {
                    return t1;
                }
                return sensorReading;
            }
        }).print("reduce");

        // 1.2 AggregateFunction , 计算条数
        source.keyBy(SensorReading::getId)
                .countWindow(5).aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(SensorReading value, Integer accumulator) {
                return accumulator + 1;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }).print("aggregate");


        // nc -L -p 9999
        SingleOutputStreamOperator<SensorReading> socketSource = env.socketTextStream("127.0.0.1", 9999)
                .flatMap(new FlatMapFunction<String, SensorReading>() {
                    @Override
                    public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                        String[] split = value.split("\\s+");
                        if (split.length == 3) {
                            out.collect(new SensorReading(split[0], split[1], Double.valueOf(split[2])));
                        }
                    }
                });

        // 2.全窗口函数,先把数据收集起来,等计算的时候遍历数据
        // 2.1 ProcessFunction
        WindowedStream<SensorReading, String, TimeWindow> timeWindow = socketSource.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(10));
        timeWindow.process(new ProcessWindowFunction<SensorReading, Tuple3<String, String, Long>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, String, Long>> out) throws Exception {
                // 输出 <key,窗口开始时间-窗口结束时间,累加值>
                long count = IterableUtils.toStream(elements).count();
                TimeWindow window = context.window();
                String start = DateUtils.formatDate(new Date(window.getStart()), "HH:mm:ss");
                String end = DateUtils.formatDate(new Date(window.getEnd()), "HH:mm:ss");

                out.collect(Tuple3.apply(key, start + " ~ " + end, count));
            }
        }).print("process");
        // 2.2 WindowFunction
        timeWindow.apply(new WindowFunction<SensorReading, Object, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<SensorReading> input, Collector<Object> out) throws Exception {
                // 输出 <key,窗口开始时间-窗口结束时间,累加值>
                long count = IterableUtils.toStream(input).count();
                String start = DateUtils.formatDate(new Date(window.getStart()), "HH:mm:ss");
                String end = DateUtils.formatDate(new Date(window.getEnd()), "HH:mm:ss");

                out.collect(Tuple3.apply(key, start + " ~ " + end, count));
            }
        }).print("apply");
    }

    private static void windowType(DataStream<SensorReading> source) {
        // 1.基于时间的窗口 进Flink时间、算子处理时间、数据自带时间(会话时间)
        WindowedStream<SensorReading, String, TimeWindow> timeWindow =
                source.keyBy(SensorReading::getId)
                        // 1.1滚动窗口
                        //.timeWindow(Time.seconds(15));
                        // 1.2滑动窗口
                        //.timeWindow(Time.seconds(15),Time.seconds(5));
                        // 1.3会话窗口
                        .window(EventTimeSessionWindows.withGap(Time.seconds(15)));


        // 2.基于次数的窗口
        source.keyBy(SensorReading::getId)
                // 1.1滚动窗口
                //.countWindow(15);
                // 1.2滑动窗口
                .countWindow(15, 5);
    }


}
