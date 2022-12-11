package com.sink;

import com.source.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.HashMap;

public class sinktoElasticsearchTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        );

//        定义Host列表
        ArrayList<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("hadoop102",9200));

//        定义ElasticSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {

            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> hashmap = new HashMap<>();

                hashmap.put(event.user, event.url);

                IndexRequest request = Requests.indexRequest().
                        index("clicks").type("type").source(hashmap);

                requestIndexer.add(request);
            }
        };


        stream.addSink(new ElasticsearchSink.Builder<>(httpHost, elasticsearchSinkFunction).build());

        env.execute();
    }
}
