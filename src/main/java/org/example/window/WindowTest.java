package org.example.window;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example.bean.SensorReading;

import java.util.Arrays;

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

        env.execute();
    }


}
