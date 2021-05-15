package org.example.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.example.bean.SensorReading;

/**
 * Table Api 入门测试
 *
 * @author liangchuanchuan
 */
public class TableApiTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings blinkStreamEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamEnvSettings);

        blinkStreamTableEnv.connect(new FileSystem().path("D:\\javaProject\\flink-demo\\src\\main\\resources\\sensorreading.data"))
                .withFormat(new Csv().fieldDelimiter(' '))
                .withSchema(new Schema()
                        .field("city", DataTypes.STRING())
                        .field("ts", DataTypes.STRING())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("sensorreading");


        Table table = blinkStreamTableEnv.sqlQuery("select * from sensorreading where temperature > 20");
        table.printSchema();

        blinkStreamTableEnv.toAppendStream(table,Row.class).print("row");

        env.execute();
        // tableApi();
        // env();
    }

    private static void env() {
        // 1.1 老版本planner流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings oldEnvironmentSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldEnvironmentSettings);

        // 1.2 老版本planner批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


        // 2.1 基于Blink的流处理
        EnvironmentSettings blinkStreamEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamEnvSettings);

        // 2.2 基于Blink的批处理
        EnvironmentSettings blinkBatchEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchEnvSettings);
    }

    private static void tableApi() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> source = env.readTextFile("D:\\javaProject\\flink-demo\\src\\main\\resources\\sensorreading.data")
                .flatMap(new FlatMapFunction<String, SensorReading>() {
                    @Override
                    public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                        String[] split = value.split("\\s+");
                        if (split.length == 3) {
                            out.collect(new SensorReading(split[0], split[1], Double.valueOf(split[2])));
                        }
                    }
                });

        // source.print("source");

        // 1.创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.使用Table Api
        Table table = tableEnv.fromDataStream(source);
        Table select = table.select("id,temperature");
        select.printSchema();
        tableEnv.toAppendStream(select, Row.class).print("tableApi");

        // 3.使用Sql
        tableEnv.createTemporaryView("sensorreading", source);
        Table select2 = tableEnv.sqlQuery("select * from sensorreading limit 2");
        select2.printSchema();
        tableEnv.toAppendStream(select2, Row.class).print("sql");

        env.execute();
    }

}
