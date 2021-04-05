package org.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.bean.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;

/**
 * 自定义jdbc sink
 * <p>
 * mysql> use test;
 * mysql> create table wen_du(id varchar(20) not null,temperature double not null);
 * mysql> truncate table wen_du;
 * mysql> select * from wen_du;
 *
 * @author liangchuanchuan
 */
public class JdbcSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> source = env.fromCollection(Arrays.asList(
                new SensorReading("guangzhou", "2021-04-01", 20.2)
                , new SensorReading("guangzhou", "2021-04-02", 24.2)
                , new SensorReading("guangzhou", "2021-04-03", 24.2)
                , new SensorReading("guangzhou", "2021-04-04", 33.6)
                , new SensorReading("guangzhou", "2021-04-05", 22.6)
                , new SensorReading("shanghai", "2021-04-05", 23.6)
                , new SensorReading("shanghai", "2021-04-05", 33.2)
        ));

        source.addSink(new RichSinkFunction<SensorReading>() {
            Connection connection = null;
            PreparedStatement insertStatement = null;
            PreparedStatement updateStatement = null;

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                updateStatement.setString(2, value.getId());
                updateStatement.setDouble(1, value.getTemperature());
                int i = updateStatement.executeUpdate();
                if (i == 0) {
                    insertStatement.setString(1, value.getId());
                    insertStatement.setDouble(2, value.getTemperature());
                    insertStatement.executeUpdate();
                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8", "root", "root");
                insertStatement = connection.prepareStatement("insert into wen_du(id,temperature) values(?,?)");
                updateStatement = connection.prepareStatement("update wen_du set temperature = ? where id = ?");
            }

            @Override
            public void close() throws Exception {
                insertStatement.close();
                updateStatement.close();
                connection.close();
            }
        });

        env.execute();
    }

}
