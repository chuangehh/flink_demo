package org.example.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.bean.SensorReading;

import java.util.Arrays;

/**
 * RedisSink
 *
 * 127.0.0.1:6379> KEYS *
 * 1) "wen_du"
 *
 * 127.0.0.1:6379> HGETALL wen_du
 * 1) "guangzhou"
 * 2) "22.6"
 * 3) "shanghai"
 * 4) "33.2"
 *
 * 127.0.0.1:6379> hget wen_du guangzhou
 * "22.6"
 *
 * @author liangchuanchuan
 */
public class RedisSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
                , new SensorReading("guangzhou", "2021-04-05", 22.6)
                , new SensorReading("shanghai", "2021-04-05", 23.6)
                , new SensorReading("shanghai", "2021-04-05", 33.2)
        ));

        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();

        // 使用hash key为城市, value为温度
        source.addSink(new RedisSink<>(flinkJedisPoolConfig, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "wen_du");
            }

            @Override
            public String getKeyFromData(SensorReading sensorReading) {
                return sensorReading.getId();
            }

            @Override
            public String getValueFromData(SensorReading sensorReading) {
                return sensorReading.getTemperature().toString();
            }
        }));

        env.execute();
    }

}
