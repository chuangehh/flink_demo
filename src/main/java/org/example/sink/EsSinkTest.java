package org.example.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.example.bean.SensorReading;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * https://www.elastic.co/cn/downloads/elasticsearch
 * <p>
 * windows双击
 * elasticsearch-7.12.0/bin/elasticsearch.bat
 * http://127.0.0.1:9200
 * <p>
 *
 *
 * 查看结果
 * http://127.0.0.1:9200/_cat/indices
 * http://127.0.0.1:9200/wen_du/_search?pretty
 *
 * @author liangchuanchuan
 */
public class EsSinkTest {

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


        List<HttpHost> httpHosts = Arrays.asList(new HttpHost("127.0.0.1", 9200));
        ElasticsearchSink build = new ElasticsearchSink.Builder(httpHosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest indexRequest = new IndexRequest();

                HashMap<String, String> data = new HashMap<>();
                data.put("id", sensorReading.getId());
                data.put("ts", sensorReading.getTimestamp());
                data.put("temperature", sensorReading.getTemperature().toString());
                indexRequest.index("wen_du")
                        .source(data);

                requestIndexer.add(indexRequest);
            }
        }).build();

        source.addSink(build);

        env.execute();
    }

}
