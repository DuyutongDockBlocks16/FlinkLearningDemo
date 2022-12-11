package com.sink;

import com.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class sinktoTextfileTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

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

        StreamingFileSink<String> streamingfilesink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8")).withRollingPolicy(
                DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .build()
        ).build();



        stream.map(data->data.toString())
                .addSink(streamingfilesink);

        env.execute();

    }
}
