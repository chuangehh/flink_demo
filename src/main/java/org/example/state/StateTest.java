package org.example.state;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.SensorReading;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 状态管理
 * <p>
 * 算子状态：列表状态（List state）,联合列表状态（Union list state），广播状态（Broadcast state）
 * 作用范围限定为算子任务
 * 同一并行任务所处理的所有数据都可以访问到相同的状态
 * 状态对于同一子任务而言是共享的，其他子任务访问不能访问
 * <p>
 * 键控状态: 值状态（Value state），列表状态（List state），映射状态（Map state），聚合状态（Reducing state & Aggregating State）
 * 根据输入数据流中定义的键（key）来维护和访问
 * <p>
 * 状态后端:
 * MemoryStateBackend: 快速、低延迟，但不稳定。将它们存储在TaskManager 的 JVM 堆上，而将 checkpoint 存储在 JobManager 的内存中
 * FsStateBackend：同时拥有内存级的本地访问速度，和更好的容错保证
 * RocksDBStateBackend： 将所有状态序列化后，存入本地的 RocksDB 中存储。
 */
public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
                , new SensorReading("11", "2021-04-05", 22.6)
        ));


        source.map(new CountMapFunction()).print();

        source.keyBy(SensorReading::getId)
                .map(new KeyMapFunction())
                .print("KeyMapFunction");
        env.execute();
    }

    /**
     * 每来一条数据计算 count
     * 默认在内存里，如果挂掉数据会丢失
     */
    static class KeyMapFunction extends RichMapFunction<SensorReading, Long> {

        // The runtime context has not been initialized.
        // ValueState<Long> state = getRuntimeContext().getState(new ValueStateDescriptor<>("11", Long.class));
        ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("11", Long.class,0L));
        }

        @Override
        public Long map(SensorReading value) throws Exception {
            Long count = state.value();
            count++;
            state.update(count);
            return state.value();
        }

    }

    static class CountMapFunction implements MapFunction<SensorReading, Long>, ListCheckpointed<Long> {
        Long count = 0L;

        @Override
        public Long map(SensorReading value) throws Exception {
            return ++count;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            for (Long num : state) {
                count += num;
            }
        }
    }


}
